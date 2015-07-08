namespace MBrace.Runtime.InMemoryRuntime

open System
open System.Collections.Generic
open System.Collections.Concurrent
open System.Threading

open MBrace.Core
open MBrace.Core.Internals

[<AutoSerializable(false)>]
type private InMemoryValue<'T> (value : 'T) =
    let id = mkUUID()

    interface ICloudValue<'T> with
        member x.Id: string = id
        member x.Size: int64 = -1L
        member x.StorageLevel : StorageLevel = StorageLevel.MemoryOnly
        member x.Type: Type = typeof<'T>
        member x.GetBoxedValueAsync(): Async<obj> = async { return box value }
        member x.GetValueAsync(): Async<'T> = async { return value }
        member x.IsCachedLocally: bool = true
        member x.Value: 'T = value
        member x.GetBoxedValue () : obj = value :> obj
        member x.Dispose() = async.Zero()

[<AutoSerializable(false)>]
type private InMemoryArray<'T> (values : 'T []) =
    inherit InMemoryValue<'T[]> (values)

    interface ICloudArray<'T> with
        member x.Length = values.Length

    interface ICloudCollection<'T> with
        
        member x.GetCount(): Async<int64> =  async { return values.LongLength }

        // TODO : move to MBRace.Runtime.Core        
        member x.GetSize(): Async<int64> = async { return values.LongLength }
        
        member x.IsKnownCount: bool = true
        
        member x.IsKnownSize: bool = true
        
        member x.IsMaterialized: bool = true
        
        member x.ToEnumerable(): Async<seq<'T>> = async { return values :> _ }


[<Sealed; AutoSerializable(false)>]
type InMemoryValueProvider () =
    let id = mkUUID()

    interface ICloudValueProvider with
        member x.Id: string = id
        member x.Name: string = "In-Memory Value Provider"
        member x.DefaultStorageLevel : StorageLevel = StorageLevel.MemoryOnly
        member x.CreateCloudValue(payload: 'T, _ : StorageLevel): Async<ICloudValue<'T>> = async {
            return new InMemoryValue<'T>(payload) :> ICloudValue<'T>
        }

        member x.CreatePartitionedArray(payload : seq<'T>, _ : StorageLevel, _) = async {
            return [| new InMemoryArray<'T>(Seq.toArray payload) :> ICloudArray<'T> |]
        }
        
        member x.Dispose(_: ICloudValue): Async<unit> = async.Zero()
        member x.DisposeAllValues(): Async<unit> = async { return () }
        member x.GetById(_:string) : Async<ICloudValue> = async { return raise <| new NotSupportedException() }
        member x.GetAllValues(): Async<ICloudValue []> = async { return raise <| new NotSupportedException() }

[<AutoSerializable(false)>]
type private InMemoryAtom<'T> (initial : 'T) =
    let id = mkUUID()
    let container = ref (Some initial)

    let rec swap (f : 'T -> 'T) = 
        match container.Value with
        | None -> raise <| new ObjectDisposedException("CloudAtom")
        | cv ->
            let result = Interlocked.CompareExchange<'T option>(container, Option.map f cv, cv)
            if obj.ReferenceEquals(result, cv) then ()
            else Thread.SpinWait 20; swap f

    let transact f =
        let cell = ref Unchecked.defaultof<'R>
        let f t = let r,t' = f t in cell := r ; t'
        swap f
        !cell

    let force (t : 'T) =
        match container.Value with
        | None -> raise <| new ObjectDisposedException("CloudAtom")
        | _ -> container := Some t

    interface ICloudAtom<'T> with
        member __.Id = id
        member __.Value = async { return Option.get container.Value }
        member __.Transact(updater, _) = async { return transact updater }
        member __.Force(value) = async { return force value }
        member __.Dispose () = async { return container := None }

[<Sealed; AutoSerializable(false)>]
type InMemoryAtomProvider () =
    let id = mkUUID()
    static member CreateConfiguration () : CloudAtomConfiguration =
        {
            AtomProvider = new InMemoryAtomProvider() :> ICloudAtomProvider
            DefaultContainer = ""
        }

    interface ICloudAtomProvider with
        member __.Name = "InMemoryAtomProvider"
        member __.Id = id
        member __.CreateUniqueContainerName () = mkUUID()
        member __.IsSupportedValue _ = true
        member __.CreateAtom<'T>(_, init : 'T) = async { return new InMemoryAtom<'T>(init) :> _ }
        member __.DisposeContainer _ = raise <| new NotImplementedException()


[<Sealed; AutoSerializable(false)>]
type private InMemoryQueue<'T>  (id : string) =
    let mutable isDisposed = false
    let checkDisposed () =
        if isDisposed then raise <| new ObjectDisposedException("Queue has been disposed.")

    let mbox = new MailboxProcessor<'T>(fun _ -> async.Zero())

    interface ICloudQueue<'T> with
        member x.Count: Async<int64> = async {
            checkDisposed()
            return int64 mbox.CurrentQueueLength
        }
        
        member x.Dequeue(?timeout: int): Async<'T> = async {
            checkDisposed()
            return! mbox.Receive(?timeout = timeout)
        }
        
        member x.Dispose(): Async<unit> = async {
            isDisposed <- true
        }
        
        member x.Enqueue(message: 'T): Async<unit> = async {
            checkDisposed()
            return mbox.Post message
        }
        
        member x.EnqueueBatch(messages: seq<'T>): Async<unit> = async {
            do
                checkDisposed()
                for m in messages do mbox.Post m
        }
        
        member x.Id: string = id
        
        member x.TryDequeue(): Async<'T option> = async {
            checkDisposed()
            return! mbox.TryReceive(timeout = 0)
        }

/// Defines an in-memory queue factory using mailbox processor
[<Sealed; AutoSerializable(false)>]
type InMemoryQueueProvider () =
    let id = mkUUID()

    static member CreateConfiguration () : CloudQueueConfiguration =
        {
            QueueProvider = new InMemoryQueueProvider() :> ICloudQueueProvider
            DefaultContainer = ""
        }

    interface ICloudQueueProvider with
        member __.Name = "InMemoryQueueProvider"
        member __.Id = id
        member __.CreateUniqueContainerName () = mkUUID()

        member __.CreateQueue<'T> (container : string) = async {
            let id = sprintf "%s/%s" container <| mkUUID()
            return new InMemoryQueue<'T>(id) :> ICloudQueue<'T>
        }

        member __.DisposeContainer _ = async.Zero()

[<Sealed; AutoSerializable(false)>]
type private InMemoryDictionary<'T>  () =
    let id = mkUUID()
    let dict = new System.Collections.Concurrent.ConcurrentDictionary<string, 'T> ()
    interface ICloudDictionary<'T> with
        member x.Add(key : string, value : 'T) : Async<unit> =
            async { return dict.[key] <- value }

        member x.TryAdd(key: string, value: 'T): Async<bool> = 
            async { return dict.TryAdd(key, value) }
                    
        member x.Transact(key: string, transacter: 'T option -> 'R * 'T, _): Async<'R> = async {
            let result = ref Unchecked.defaultof<'R>
            let updater (curr : 'T option) =
                let r, topt = transacter curr
                result := r
                topt

            let _ = dict.AddOrUpdate(key, (fun _ -> updater None), fun _ curr -> updater (Some curr))
            return result.Value
        }
                    
        member x.ContainsKey(key: string): Async<bool> = 
            async { return dict.ContainsKey key }

        member x.IsKnownCount = true
        member x.IsKnownSize = true
        member x.IsMaterialized = true
                    
        member x.GetCount(): Async<int64> = 
            async { return int64 dict.Count }

        member x.GetSize(): Async<int64> = 
            async { return int64 dict.Count }
                    
        member x.Dispose(): Async<unit> = async.Zero()

        member x.Id: string = id
                    
        member x.Remove(key: string): Async<bool> = 
            async { return dict.TryRemove key |> fst }
                    
        member x.ToEnumerable(): Async<seq<KeyValuePair<string, 'T>>> = 
            async { return dict :> _ }
                    
        member x.TryFind(key: string): Async<'T option> = 
            async { return let ok,v = dict.TryGetValue key in if ok then Some v else None }

/// Defines an in-memory dictionary factory using ConcurrentDictionary
[<Sealed; AutoSerializable(false)>]
type InMemoryDictionaryProvider() =
    let id = mkUUID()
    interface ICloudDictionaryProvider with
        member s.Name = "InMemoryDictionaryProvider"
        member s.Id = id
        member s.IsSupportedValue _ = true
        member s.Create<'T> () = async {
            return new InMemoryDictionary<'T>() :> ICloudDictionary<'T>
        }