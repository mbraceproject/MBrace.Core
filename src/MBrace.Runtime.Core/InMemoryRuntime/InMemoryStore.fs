namespace MBrace.Runtime.InMemoryRuntime

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

open MBrace.Runtime.Utils
open MBrace.Runtime.Vagabond
open MBrace.Runtime.Vagabond.FsPicklerExtensions

[<AutoSerializable(false); CloneableOnly>]
type private InMemoryValue<'T> (hash : HashResult, provider : InMemoryValueProvider) =

    let getPayload() =
        match provider.TryGetPayload hash with
        | Some p -> p
        | None -> raise <| new ObjectDisposedException(hash.Id, "CloudValue has been disposed.")

    /// Constructor that ensures arrays are initialized as ICloudArray instances
    static member internal Create(hash : HashResult, payload : EmulatedValue<obj>, provider : InMemoryValueProvider) : InMemoryValue<'T> =
        let _ = payload.RawValue :?> 'T
        let t = typeof<'T>
        if t.IsArray && t.GetArrayRank() = 1 then
            let et = t.GetElementType()
            let eet = Existential.FromType et
            eet.Apply 
                { new IFunc<InMemoryValue<'T>> with 
                    member __.Invoke<'et> () =
                        let imv = new InMemoryArray<'et>(hash, provider) 
                        imv :> ICloudValue :?> InMemoryValue<'T> }
        else
            new InMemoryValue<'T>(hash, provider)

    /// Constructor that ensures arrays are initialized as ICloudArray instances
    static member internal CreateUntyped(hash : HashResult, payload : EmulatedValue<obj>, provider : InMemoryValueProvider) : ICloudValue =
        let t = getReflectedType payload.RawValue
        let et = Existential.FromType t
        et.Apply { new IFunc<ICloudValue> with member __.Invoke<'T> () = InMemoryValue<'T>.Create(hash, payload, provider) :> ICloudValue }

    interface CloudValue<'T> with
        member x.Id: string = hash.Id
        member x.Size: int64 = hash.Length
        member x.StorageLevel : StorageLevel =
            match getPayload() with
            | Shared _ -> StorageLevel.Memory
            | Cloned _ -> StorageLevel.MemorySerialized

        member x.Type: Type = typeof<'T>
        member x.ReflectedType : Type = getReflectedType <| getPayload().RawValue
        member x.GetBoxedValueAsync(): Async<obj> = async { return getPayload().Value }
        member x.GetValueAsync(): Async<'T> = async { return getPayload().Value :?> 'T }
        member x.IsCachedLocally: bool = not <| provider.IsDisposed hash
        member x.Value: 'T = getPayload().Value :?> 'T
        member x.GetBoxedValue () : obj = getPayload().Value
        member x.Cast<'S> () = InMemoryValue<'S>.Create(hash, getPayload(), provider) :> CloudValue<'S>
        member x.Dispose() = async { provider.Dispose hash }

and [<AutoSerializable(false); Sealed; CloneableOnly>]
  private InMemoryArray<'T> (hash : HashResult, provider : InMemoryValueProvider) =
    inherit InMemoryValue<'T[]> (hash, provider)

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
  InMemoryValueProvider () as self =
    let id = mkUUID()
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
        | None -> raise <| new ObjectDisposedException(id, "CloudValue could not be found in store.")
        | Some payload -> InMemoryValue<_>.CreateUntyped(hash, payload, self)

    let createNewValue (level : StorageLevel) (value : 'T) =
        let hash = computeHash value
        let createPayload _ =
            let mode =
                if level.HasFlag StorageLevel.MemorySerialized then MemoryEmulation.Copied
                else MemoryEmulation.Shared

            EmulatedValue.create mode false (box value)

        let payload = cache.GetOrAdd(hash, createPayload)
        InMemoryValue<'T>.Create(hash, payload, self) :> CloudValue<'T>

    member internal __.TryGetPayload(hash : HashResult) : EmulatedValue<obj> option = cache.TryFind hash
    member internal __.IsDisposed(hash : HashResult) = not <| cache.ContainsKey hash
    member internal __.Dispose(hash : HashResult) = 
        let mutable v = Unchecked.defaultof<_> 
        let _ = cache.TryRemove(hash, &v)
        ()

    interface ICloudValueProvider with
        member x.Id: string = id
        member x.Name: string = "In-Memory Value Provider"
        member x.DefaultStorageLevel : StorageLevel = StorageLevel.Memory
        member x.IsSupportedStorageLevel (level : StorageLevel) = isSupportedLevel level
        member x.CreateCloudValue(payload: 'T, level : StorageLevel): Async<CloudValue<'T>> = async {
            if not <| isSupportedLevel level then invalidArg "level" <| sprintf "Unsupported storage level '%O'." level
            return createNewValue level payload
        }

        member x.CreateCloudArrayPartitioned(sequence : seq<'T>, partitionThreshold : int64, level : StorageLevel) = async {
            if not <| isSupportedLevel level then invalidArg "level" <| sprintf "Unsupported storage level '%O'." level
            return! FsPickler.PartitionSequenceBySize(sequence, partitionThreshold, fun ts -> async { return createNewValue level ts :?> CloudArray<'T> })
        }
        
        member x.Dispose(cv: ICloudValue): Async<unit> = async { return! cv.Dispose() }
        member x.DisposeAllValues(): Async<unit> = async { return cache.Clear() }
        member x.GetValueById(id : string) : Async<ICloudValue> = async { return getValueById id }
        member x.GetAllValues(): Async<ICloudValue []> = async { return cache |> Seq.map (fun kv -> InMemoryValue<_>.CreateUntyped(kv.Key, kv.Value, x)) |> Seq.toArray }


[<AutoSerializable(false); Sealed; CloneableOnly>]
type private InMemoryAtom<'T> internal (id : string, initial : 'T, mode : MemoryEmulation) =
    // false: always clone value when reading payload
    let clone (t:'T) = EmulatedValue.create mode false t

    let mutable atom = Some <| Atom.create (clone initial)
    let getAtom() =
        match atom with
        | None -> raise <| new ObjectDisposedException(id, "CloudAtom has been disposed.")
        | Some a -> a

    interface CloudAtom<'T> with
        member x.Id = id
        member x.Value = async { return getAtom().Value.Value }

        member x.Transact(updater : 'T -> 'R * 'T, _) = async { 
            let transacter (ct : EmulatedValue<'T>) : EmulatedValue<'T> * 'R =
                let r,t' = updater ct.Value
                clone t', r

            return getAtom().Transact transacter
        }

        member x.Force(value:'T) = async { 
            return getAtom().Force(clone value)
        }

        member x.Dispose () = async { return atom <- None }

[<Sealed; AutoSerializable(false)>]
type InMemoryAtomProvider (mode : MemoryEmulation) =
    let id = mkUUID()

    /// <summary>
    ///     Creates an In-Memory Atom configuration object.
    /// </summary>
    /// <param name="mode">Memory emulation mode.</param>
    static member CreateConfiguration(mode : MemoryEmulation) =
        let imap = new InMemoryAtomProvider(mode)
        CloudAtomConfiguration.Create(imap)

    interface ICloudAtomProvider with
        member __.Name = "InMemoryAtomProvider"
        member __.Id = id
        member __.IsSupportedValue _ = true
        member __.CreateAtom<'T>(_, init : 'T) = async { 
            let id = mkUUID()
            return new InMemoryAtom<'T>(id, init, mode) :> _ 
        }

        member __.CreateUniqueContainerName () = mkUUID()
        member __.DisposeContainer _ = raise <| new NotSupportedException()


[<Sealed; AutoSerializable(false); CloneableOnly>]
type private InMemoryQueue<'T> internal (id : string, mode : MemoryEmulation) =
    // true: value will be dequeued only once so clone on eqnueue only
    let clone (t : 'T) = EmulatedValue.create mode true t
    let mutable isDisposed = false
    let checkDisposed () =
        if isDisposed then raise <| new ObjectDisposedException(id, "CloudQueue has been disposed.")

    let mbox = new MailboxProcessor<EmulatedValue<'T>>(fun _ -> async.Zero())

    interface CloudQueue<'T> with
        member x.Id: string = id

        member x.Count: Async<int64> = async {
            checkDisposed()
            return int64 mbox.CurrentQueueLength
        }
        
        member x.Dequeue(?timeout: int): Async<'T> = async {
            checkDisposed()
            let! ev = mbox.Receive(?timeout = timeout)
            return ev.Value
        }
        
        member x.Dispose(): Async<unit> = async {
            isDisposed <- true
        }
        
        member x.Enqueue(message: 'T): Async<unit> = async {
            checkDisposed()
            return mbox.Post (clone message)
        }
        
        member x.EnqueueBatch(messages: seq<'T>): Async<unit> = async {
            do
                checkDisposed()
                for m in messages do mbox.Post (clone m)
        }
        
        member x.TryDequeue(): Async<'T option> = async {
            checkDisposed()
            let! result = mbox.TryReceive(timeout = 0)
            return result |> Option.map (fun r -> r.Value)
        }

/// Defines an in-memory queue factory using mailbox processor
[<Sealed; AutoSerializable(false)>]
type InMemoryQueueProvider (mode : MemoryEmulation) =
    let id = mkUUID()

    /// <summary>
    ///     Creates an In-Memory Queue configuration object.
    /// </summary>
    /// <param name="mode">Memory emulation mode.</param>
    static member CreateConfiguration(mode : MemoryEmulation) =
        let imqp = new InMemoryQueueProvider(mode)
        CloudQueueConfiguration.Create(imqp)

    interface ICloudQueueProvider with
        member __.Name = "InMemoryQueueProvider"
        member __.Id = id
        member __.CreateUniqueContainerName () = mkUUID()

        member __.CreateQueue<'T> (container : string) = async {
            if not <| MemoryEmulation.isShared mode && not <| FsPickler.IsSerializableType<'T>() then
                let msg = sprintf "Cannot create queue for non-serializable type '%O'." typeof<'T>
                raise <| new SerializationException(msg)

            let id = sprintf "%s/%s" container <| mkUUID()
            return new InMemoryQueue<'T>(id, mode) :> CloudQueue<'T>
        }

        member __.DisposeContainer _ = async.Zero()

[<Sealed; AutoSerializable(false); CloneableOnly>]
type private InMemoryDictionary<'T> internal (mode : MemoryEmulation) =
    let id = mkUUID()
    let clone (t:'T) = EmulatedValue.create mode true t
    let dict = new ConcurrentDictionary<string, EmulatedValue<'T>> ()
    let toEnum() = dict |> Seq.map (fun kv -> new KeyValuePair<_,_>(kv.Key, kv.Value.Value))
    let mutable isDisposed = false
    let checkDisposed() = 
        if isDisposed then raise <| new ObjectDisposedException(id, "CloudDictionary has been disposed.")

    interface seq<KeyValuePair<string, 'T>> with
        member x.GetEnumerator() = checkDisposed() ; toEnum().GetEnumerator() :> Collections.IEnumerator
        member x.GetEnumerator() = checkDisposed() ; toEnum().GetEnumerator()
    
    interface CloudDictionary<'T> with
        member x.Add(key : string, value : 'T) : Async<unit> =
            async { let _ = checkDisposed() in return dict.[key] <- clone value }

        member x.TryAdd(key: string, value: 'T): Async<bool> = 
            async { let _ = checkDisposed() in return dict.TryAdd(key, clone value) }
                    
        member x.Transact(key: string, transacter: 'T option -> 'R * 'T, _): Async<'R> = async {
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
                    
        member x.ContainsKey(key: string): Async<bool> = 
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
                    
        member x.Remove(key: string): Async<bool> = 
            async { let _ = checkDisposed() in return dict.TryRemove key |> fst }
                    
        member x.ToEnumerable(): Async<seq<KeyValuePair<string, 'T>>> = 
            async { let _ = checkDisposed() in return toEnum() }
                    
        member x.TryFind(key: string): Async<'T option> = 
            async { let _ = checkDisposed() in return let ok,v = dict.TryGetValue key in if ok then Some v.Value else None }

/// Defines an in-memory dictionary factory using ConcurrentDictionary
[<Sealed; AutoSerializable(false)>]
type InMemoryDictionaryProvider (mode : MemoryEmulation) =
    let id = mkUUID()

    interface ICloudDictionaryProvider with
        member s.Name = "InMemoryDictionaryProvider"
        member s.Id = id
        member s.IsSupportedValue _ = true
        member s.Create<'T> () = async {
            if not <| MemoryEmulation.isShared mode && not <| FsPickler.IsSerializableType<'T>() then
                let msg = sprintf "Cannot create queue for non-serializable type '%O'." typeof<'T>
                raise <| new SerializationException(msg)

            return new InMemoryDictionary<'T>(mode) :> CloudDictionary<'T>
        }