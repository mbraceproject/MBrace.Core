namespace MBrace.ThreadPool.Internals

open System
open System.Collections.Generic
open System.Collections.Concurrent
open System.Runtime.Serialization
open System.Threading
open System.Threading.Tasks

open Nessos.FsPickler
open Nessos.FsPickler.Hashing

open MBrace.Core
open MBrace.Core.Internals

open MBrace.Runtime
open MBrace.Runtime.Utils

[<NoEquality; NoComparison; AutoSerializable(false)>]
type internal EmulatedValue<'T> =
    | Shared of 'T
    | Cloned of 'T
with
    member inline ev.Value =
        match ev with
        | Shared t -> t
        | Cloned t -> FsPickler.Clone t

    member inline ev.RawValue =
        match ev with
        | Shared t -> t
        | Cloned t -> t

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module private MemoryEmulation =

    let isShared (mode : MemoryEmulation) =
        match mode with
        | MemoryEmulation.EnsureSerializable
        | MemoryEmulation.Copied -> false
        | _ -> true

module private EmulatedValue =

    /// Performs cloning of value based on emulation semantics
    let clone (mode : MemoryEmulation) (value : 'T) : 'T =
        match mode with
        | MemoryEmulation.Copied -> 
            FsPickler.Clone value

        | MemoryEmulation.EnsureSerializable ->
            FsPickler.EnsureSerializable(value, failOnCloneableOnlyTypes = false)
            value

        | MemoryEmulation.Shared 
        | _ -> value
    
    /// Creates a copy of provided value given emulation semantics
    let create (mode : MemoryEmulation) shareCloned (value : 'T) =
        match mode with
        | MemoryEmulation.Copied -> 
            let clonedVal = FsPickler.Clone value
            if shareCloned then Shared clonedVal
            else Cloned clonedVal

        | MemoryEmulation.EnsureSerializable ->
            FsPickler.EnsureSerializable(value, failOnCloneableOnlyTypes = false)
            Shared value

        | MemoryEmulation.Shared 
        | _ -> Shared value

[<AutoSerializable(false); CloneableOnly>]
type private ThreadPoolValue<'T> (hash : HashResult, provider : ThreadPoolValueProvider) =

    let getPayload() =
        match provider.TryGetPayload hash with
        | Some p -> p
        | None -> raise <| new ObjectDisposedException(hash.Id, "CloudValue has been disposed.")

    /// Constructor that ensures arrays are initialized as ICloudArray instances
    static member internal Create(hash : HashResult, payload : EmulatedValue<obj>, provider : ThreadPoolValueProvider) : ThreadPoolValue<'T> =
        let _ = payload.RawValue :?> 'T
        let t = typeof<'T>
        if t.IsArray && t.GetArrayRank() = 1 then
            let et = t.GetElementType()
            let eet = Existential.FromType et
            eet.Apply 
                { new IFunc<ThreadPoolValue<'T>> with 
                    member __.Invoke<'et> () =
                        let imv = new ThreadPoolArray<'et>(hash, provider) 
                        imv :> ICloudValue :?> ThreadPoolValue<'T> }
        else
            new ThreadPoolValue<'T>(hash, provider)

    /// Constructor that ensures arrays are initialized as ICloudArray instances
    static member internal CreateUntyped(hash : HashResult, payload : EmulatedValue<obj>, provider : ThreadPoolValueProvider) : ICloudValue =
        let t = getReflectedType payload.RawValue
        let et = Existential.FromType t
        et.Apply { new IFunc<ICloudValue> with member __.Invoke<'T> () = ThreadPoolValue<'T>.Create(hash, payload, provider) :> ICloudValue }

    interface CloudValue<'T> with
        member x.Id: string = hash.Id
        member x.Size: int64 = hash.Length
        member x.StorageLevel : StorageLevel =
            match getPayload() with
            | Shared _ -> StorageLevel.Memory
            | Cloned _ -> StorageLevel.MemorySerialized

        member x.Type: Type = typeof<'T>
        member x.ReflectedType : Type = getReflectedType <| getPayload().RawValue
        member x.GetValueBoxedAsync(): Async<obj> = async { return getPayload().Value }
        member x.GetValueAsync(): Async<'T> = async { return getPayload().Value :?> 'T }
        member x.IsCachedLocally: bool = not <| provider.IsDisposed hash
        member x.Value: 'T = getPayload().Value :?> 'T
        member x.ValueBoxed: obj = getPayload().Value
        member x.Cast<'S> () = ThreadPoolValue<'S>.Create(hash, getPayload(), provider) :> CloudValue<'S>
        member x.Dispose() = async { provider.Dispose hash }

and [<AutoSerializable(false); Sealed; CloneableOnly>]
  private ThreadPoolArray<'T> (hash : HashResult, provider : ThreadPoolValueProvider) =
    inherit ThreadPoolValue<'T[]> (hash, provider)

    let getPayload() =
        match provider.TryGetPayload hash with
        | Some p -> p
        | None -> raise <| new ObjectDisposedException(hash.Id, "CloudValue has been disposed.")

    let checkDisposed () =
        if provider.IsDisposed hash then raise <| new ObjectDisposedException(hash.Id, "CloudValue payload has been disposed.")

    interface CloudArray<'T> with
        member x.Length = (getPayload().RawValue :?> 'T[]).Length

    interface seq<'T> with
        member x.GetEnumerator(): Collections.IEnumerator = (getPayload().Value :?> 'T[]).GetEnumerator()
        member x.GetEnumerator(): IEnumerator<'T> = (getPayload().Value :?> seq<'T>).GetEnumerator()

    interface ICloudCollection<'T> with
        member x.IsKnownCount: bool = checkDisposed () ; true
        member x.IsKnownSize: bool = checkDisposed () ; true
        member x.IsMaterialized: bool = checkDisposed () ; true
        member x.GetCount(): Async<int64> =  async { return (getPayload().RawValue :?> 'T[]).LongLength }
        member x.GetSize(): Async<int64> = async { let _ = checkDisposed () in return hash.Length }
        member x.ToEnumerable(): Async<seq<'T>> = async { return getPayload().Value :?> seq<'T> }

/// Provides an In-Memory CloudValue implementation
and [<Sealed; AutoSerializable(false)>] 
  ThreadPoolValueProvider () as self =
    let id = sprintf "inMemoryCloudValueProvider-%s" <| mkUUID()
    let cache = new ConcurrentDictionary<HashResult, EmulatedValue<obj>>()

    static let isSupportedLevel (level : StorageLevel) =
        // while this implementation does not support disk persistence, tolerate inputs for compatibility reasons
        level.HasFlag StorageLevel.Memory || level.HasFlag StorageLevel.MemorySerialized || level.HasFlag StorageLevel.Disk

    static let computeHash (payload : 'T) =
        try FsPickler.ComputeHash payload
        with e ->
            let msg = sprintf "Value '%A' is not serializable." payload
            raise <| new SerializationException(msg, e)

    let getValueById (id:string) =
        let hash = HashResult.Parse id
        match cache.TryFind hash with
        | None -> None
        | Some payload -> ThreadPoolValue<_>.CreateUntyped(hash, payload, self) |> Some

    let createNewValue (level : StorageLevel) (value : 'T) =
        let hash = computeHash value
        let createPayload _ =
            let memoryEmulation =
                if level.HasFlag StorageLevel.MemorySerialized then MemoryEmulation.Copied
                else MemoryEmulation.Shared

            EmulatedValue.create memoryEmulation false (box value)

        let payload = cache.GetOrAdd(hash, createPayload)
        ThreadPoolValue<'T>.Create(hash, payload, self) :> CloudValue<'T>

    member internal __.TryGetPayload(hash : HashResult) : EmulatedValue<obj> option = cache.TryFind hash
    member internal __.IsDisposed(hash : HashResult) = not <| cache.ContainsKey hash
    member internal __.Dispose(hash : HashResult) = 
        let mutable v = Unchecked.defaultof<_> 
        let _ = cache.TryRemove(hash, &v)
        ()

    interface ICloudValueProvider with
        member x.Id: string = id
        member x.Name: string = "ThreadPool In-Memory CloudValue Provider"
        member x.DefaultStorageLevel : StorageLevel = StorageLevel.Memory
        member x.IsSupportedStorageLevel (level : StorageLevel) = isSupportedLevel level
        member x.GetCloudValueId (value : 'T) = computeHash(value).Id
        member x.CreateCloudValue(payload: 'T, level : StorageLevel): Async<CloudValue<'T>> = async {
            if not <| isSupportedLevel level then invalidArg "level" <| sprintf "Unsupported storage level '%O'." level
            return createNewValue level payload
        }

        member x.CreateCloudArrayPartitioned(sequence : seq<'T>, partitionThreshold : int64, level : StorageLevel) = async {
            if not <| isSupportedLevel level then invalidArg "level" <| sprintf "Unsupported storage level '%O'." level
            let createArray (ts : 'T[]) = async { return createNewValue level ts :?> CloudArray<'T> }
            return! FsPickler.PartitionSequenceBySize(sequence, partitionThreshold, createArray)
        }
        
        member x.Dispose(cv: ICloudValue): Async<unit> = async { return! cv.Dispose() }
        member x.DisposeAllValues(): Async<unit> = async { return cache.Clear() }
        member x.TryGetCloudValueById(id : string) : Async<ICloudValue option> = async { return getValueById id }
        member x.GetAllCloudValues(): Async<ICloudValue []> = async { return cache |> Seq.map (fun kv -> ThreadPoolValue<_>.CreateUntyped(kv.Key, kv.Value, x)) |> Seq.toArray }


[<AutoSerializable(false); Sealed; CloneableOnly>]
type private ThreadPoolAtom<'T> internal (id : string, container : string, initial : 'T, memoryEmulation : MemoryEmulation) =
    // false: always clone value when reading payload
    let clone (t:'T) = EmulatedValue.create memoryEmulation false t

    let mutable atom = Some <| Atom.create (clone initial)
    let getAtom() =
        match atom with
        | None -> raise <| new ObjectDisposedException(id, "CloudAtom has been disposed.")
        | Some a -> a

    interface CloudAtom<'T> with
        member x.Id = id
        member x.Container = container
        member x.Value = getAtom().Value.Value
        member x.GetValueAsync() = async { return getAtom().Value.Value }

        member x.TransactAsync(updater : 'T -> 'R * 'T, _) = async { 
            let transacter (ct : EmulatedValue<'T>) : EmulatedValue<'T> * 'R =
                let r,t' = updater ct.Value
                clone t', r

            return getAtom().Transact transacter
        }

        member x.ForceAsync(value:'T) = async { 
            return getAtom().Force(clone value)
        }

        member x.Dispose () = async { return atom <- None }

[<Sealed; AutoSerializable(false)>]
type ThreadPoolAtomProvider (memoryEmulation : MemoryEmulation) =
    let id = sprintf "threadPoolCloudAtomProvider-%s" <| mkUUID()

    interface ICloudAtomProvider with
        member __.Name = "ThreadPool In-Memory CloudAtom Provider"
        member __.Id = id
        member __.DefaultContainer = "<CloudAtom Container>"
        member __.GetRandomContainerName () = "<CloudAtom Container>"
        member __.GetRandomAtomIdentifier() = sprintf "threadPoolCloudAtom-%s" <| mkUUID()
        member __.WithDefaultContainer _container = __ :> _
        member __.IsSupportedValue _value = true
        member __.CreateAtom<'T>(container : string, id : string, init : 'T) = async { 
            return new ThreadPoolAtom<'T>(id, container, init, memoryEmulation) :> _ 
        }

        member x.GetAtomById(_container: string, _atomId: string): Async<CloudAtom<'T>> =
            raise (System.NotSupportedException("Named lookups not supported in ThreadPool CloudAtoms."))

        member __.DisposeContainer _ = async.Zero()


[<Sealed; AutoSerializable(false); CloneableOnly>]
type private ThreadPoolQueue<'T> internal (id : string, memoryEmulation : MemoryEmulation) =
    // true: value will be dequeued only once so clone on eqnueue only
    let clone (t : 'T) = EmulatedValue.create memoryEmulation true t
    let mutable isDisposed = false
    let checkDisposed () =
        if isDisposed then raise <| new ObjectDisposedException(id, "CloudQueue has been disposed.")

    let mbox = new MailboxProcessor<EmulatedValue<'T>>(fun _ -> async.Zero())

    interface CloudQueue<'T> with
        member x.Id: string = id

        member x.GetCountAsync() : Async<int64> = async {
            checkDisposed()
            return int64 mbox.CurrentQueueLength
        }
        
        member x.DequeueAsync(?timeout: int): Async<'T> = async {
            checkDisposed()
            let! ev = mbox.Receive(?timeout = timeout)
            return ev.Value
        }

        member x.DequeueBatchAsync(maxItems : int) : Async<'T []> = async {
            let acc = new ResizeArray<'T> ()
            let rec aux () = async {
                if acc.Count < maxItems then
                    let! t = mbox.TryReceive(timeout = 10)
                    match t with
                    | Some t -> acc.Add t.Value ; return! aux ()
                    | None -> return ()
                else
                    return ()
            }

            do! aux ()
            return acc.ToArray()
        }
        
        member x.Dispose(): Async<unit> = async {
            isDisposed <- true
        }
        
        member x.EnqueueAsync(message: 'T): Async<unit> = async {
            checkDisposed()
            return mbox.Post (clone message)
        }
        
        member x.EnqueueBatchAsync(messages: seq<'T>): Async<unit> = async {
            checkDisposed()
            do for m in messages do mbox.Post (clone m)
        }
        
        member x.TryDequeueAsync(): Async<'T option> = async {
            checkDisposed()
            let! result = mbox.TryReceive(timeout = 0)
            return result |> Option.map (fun r -> r.Value)
        }

/// Defines an in-memory queue factory using mailbox processor
[<Sealed; AutoSerializable(false)>]
type ThreadPoolQueueProvider (memoryEmulation : MemoryEmulation) =
    let id = sprintf "threadPoolQueueProvider-%s" <| mkUUID()

    interface ICloudQueueProvider with
        member x.GetQueueById(_queueId: string): Async<CloudQueue<'T>> = 
            raise (System.NotSupportedException("Named queue lookups not supported in ThreadPool runtimes."))
        
        member x.GetRandomQueueName(): string = sprintf "threadPoolQueue-%s" <| mkUUID()
        
        member x.Id: string = id
        
        member x.Name: string = "ThreadPool InMemory CloudQueue Provider"

        member __.CreateQueue<'T> (queueId : string) = async {
            if not <| MemoryEmulation.isShared memoryEmulation && not <| FsPickler.IsSerializableType<'T>() then
                let msg = sprintf "Cannot create queue for non-serializable type '%O'." typeof<'T>
                raise <| new SerializationException(msg)

            return new ThreadPoolQueue<'T>(queueId, memoryEmulation) :> CloudQueue<'T>
        }


[<Sealed; AutoSerializable(false); CloneableOnly>]
type private InMemoryDictionary<'T> internal (id : string, memoryEmulation : MemoryEmulation) =
    let clone (t:'T) = EmulatedValue.create memoryEmulation true t
    let dict = new ConcurrentDictionary<string, EmulatedValue<'T>> ()
    let toEnum() = dict |> Seq.map (fun kv -> new KeyValuePair<_,_>(kv.Key, kv.Value.Value))
    let mutable isDisposed = false
    let checkDisposed() = 
        if isDisposed then raise <| new ObjectDisposedException(id, "CloudDictionary has been disposed.")

    interface seq<KeyValuePair<string, 'T>> with
        member x.GetEnumerator() = checkDisposed() ; toEnum().GetEnumerator() :> Collections.IEnumerator
        member x.GetEnumerator() = checkDisposed() ; toEnum().GetEnumerator()
    
    interface CloudDictionary<'T> with
        member x.AddAsync(key : string, value : 'T) : Async<unit> =
            async { let _ = checkDisposed() in return dict.[key] <- clone value }

        member x.TryAddAsync(key: string, value: 'T): Async<bool> = 
            async { let _ = checkDisposed() in return dict.TryAdd(key, clone value) }
                    
        member x.TransactAsync(key: string, transacter: 'T option -> 'R * 'T, _): Async<'R> = async {
            checkDisposed()
            let result = ref Unchecked.defaultof<'R>
            let updater (curr : EmulatedValue<'T> option) =
                let currv = curr |> Option.map (fun c -> c.Value)
                let r, t = transacter currv
                result := r
                clone t

            let _ = dict.AddOrUpdate(key, (fun _ -> updater None), fun _ curr -> updater (Some curr))
            return result.Value
        }
                    
        member x.ContainsKeyAsync(key: string): Async<bool> = 
            async { let _ = checkDisposed() in return dict.ContainsKey key }

        member x.IsKnownCount = checkDisposed(); true
        member x.IsKnownSize = checkDisposed(); true
        member x.IsMaterialized = checkDisposed(); true
                    
        member x.GetCount(): Async<int64> = 
            async { let _ = checkDisposed() in return int64 dict.Count }

        member x.GetSize(): Async<int64> = 
            async { let _ = checkDisposed() in return int64 dict.Count }
                    
        member x.Dispose(): Async<unit> = async { isDisposed <- true }

        member x.Id: string = id
                    
        member x.RemoveAsync(key: string): Async<bool> = 
            async { let _ = checkDisposed() in return dict.TryRemove key |> fst }
                    
        member x.ToEnumerable(): Async<seq<KeyValuePair<string, 'T>>> = 
            async { let _ = checkDisposed() in return toEnum() }
                    
        member x.TryFindAsync(key: string): Async<'T option> = 
            async { let _ = checkDisposed() in return let ok,v = dict.TryGetValue key in if ok then Some v.Value else None }

/// Defines an in-memory dictionary factory using ConcurrentDictionary
[<Sealed; AutoSerializable(false)>]
type ThreadPoolDictionaryProvider (memoryEmulation : MemoryEmulation) =
    let id = sprintf "threadPoolCloudDictionaryProvider-%s" <| mkUUID()

    interface ICloudDictionaryProvider with
        member s.Name = "ThreadPool In-Memory CloudDictionary Provider"
        member s.Id = id
        member s.IsSupportedValue _ = true
        member s.GetRandomDictionaryId() = sprintf "threadPoolCloudDictionary-%s" <| mkUUID()
        member s.CreateDictionary<'T> (dictId : string) = async {
            if not <| MemoryEmulation.isShared memoryEmulation && not <| FsPickler.IsSerializableType<'T>() then
                let msg = sprintf "Cannot create queue for non-serializable type '%O'." typeof<'T>
                raise <| new SerializationException(msg)

            return new InMemoryDictionary<'T>(dictId, memoryEmulation) :> CloudDictionary<'T>
        }

        member s.GetDictionaryById(_dictId : string) =
            raise (System.NotSupportedException("Named lookups not supported in ThreadPool CloudDictionaries."))