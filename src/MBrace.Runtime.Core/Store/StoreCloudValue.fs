namespace MBrace.Runtime.Store

open System
open System.Collections.Concurrent
open System.Runtime.Serialization

open Nessos.FsPickler

open MBrace.Core
open MBrace.Core.Internals

open MBrace.Runtime.Utils
open MBrace.Runtime.Utils.String
open MBrace.Runtime.Vagabond

[<AutoSerializable(false); NoEquality; NoComparison>]
type private StoreCloudValueConfiguration =
    {
        Id : string
        FileStore : ICloudFileStore
        StoreContainer : string
        Serializer : ISerializer option
        LocalCache : InMemoryCache
    }

type private StoreCloudValueRegistry private () =
    static let container = new ConcurrentDictionary<string, StoreCloudValueConfiguration> ()
    static member Register(config : StoreCloudValueConfiguration) = container.[config.Id] <- config
    static member GetById(id : string) = container.[id]
    static member RemoveById(id : string) = container.TryRemove id


[<Sealed; DataContract>]
type private StoreCloudValue<'T>(id : string, size : int64, filePath : string, config : StoreCloudValueConfiguration) =

    [<DataMember(Name = "Id")>]
    let id = id

    [<DataMember(Name = "Size")>]
    let size = size

    [<DataMember(Name = "FilePath")>]
    let filePath = filePath

    [<DataMember(Name = "InMemory")>]
    let configId = config.Id

    [<IgnoreDataMember>]
    let mutable config = config

    let getValue () = async {
        match config.LocalCache.TryFind id with
        | Some o -> return o :?> 'T
        | None ->
            use! fs = config.FileStore.BeginRead filePath
            let value =
                match config.Serializer with
                | None -> VagabondRegistry.Instance.Serializer.Deserialize<'T>(fs)
                | Some s -> s.Deserialize<'T>(fs, leaveOpen = true)

            return
                if config.LocalCache.Add(id, value) then value
                else config.LocalCache.Get id :?> 'T
    }

    [<OnDeserialized>]
    member private __.OnDeserialized (_ : StreamingContext) =
        config <- StoreCloudValueRegistry.GetById configId

    interface ICloudValue<'T> with
        member x.Dispose(): Local<unit> = local {
            do! CloudFile.Delete filePath
        }
        
        member x.GetBoxedValueAsync(): Async<obj> = async {
            let! t = getValue ()
            return box t
        }
        
        member x.GetValueAsync(): Async<'T> = getValue()
        
        member x.Id: string = id
        
        member x.IsCachedLocally: bool = config.LocalCache.ContainsKey id
        
        member x.Size: int64 = size
        
        member x.StorageLevel: StorageLevel = StorageLevel.MemoryAndDisk
        
        member x.Type: Type = typeof<'T>
        
        member x.Value: 'T =
            match config.LocalCache.TryFind id with
            | Some o -> o :?> 'T
            | None -> getValue() |> Async.RunSync
        
        member x.ValueBoxed: obj = 
            getValue() |> Async.RunSync |> box


type StoreCloudValueProvider private (config : StoreCloudValueConfiguration) =
    let instances = new ConcurrentDictionary<string, ICloudValue> ()

    let getObjectId (obj:'T) =
        let truncate (n : int) (txt : string) =
            if n <= txt.Length then txt
            else txt.Substring(0, n)

        let hash = VagabondRegistry.Instance.Serializer.ComputeHash obj
        let base32Enc = Convert.BytesToBase32 (Array.append (BitConverter.GetBytes hash.Length) hash.Hash)
        sprintf "%s-%s" (truncate 10 hash.Type) base32Enc
    
    interface ICloudValueProvider with
        member x.CreateCloudValue(payload: 'T): Async<ICloudValue<'T>> = async {
            
        
        }
        
        member x.Dispose(value: ICloudValue): Async<unit> = 
            failwith "Not implemented yet"
        
        member x.DisposeAllValues(): Async<unit> = 
            failwith "Not implemented yet"
        
        member x.GetAllValues(): Async<ICloudValue> = 
            failwith "Not implemented yet"
        
        member x.Id: string = 
            failwith "Not implemented yet"
        
        member x.Name: string = 
            failwith "Not implemented yet"
        