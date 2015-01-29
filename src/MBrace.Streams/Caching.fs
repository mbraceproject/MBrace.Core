namespace MBrace.Streams

open MBrace
open System
open System.Collections
open System.Collections.Generic
open System.Runtime.Caching
open System.Runtime.Serialization

type CachedCloudArray<'T>(source : CloudArray<'T>, taskId : string) = 
    inherit CloudArray<'T>(source.Descriptor)

    member internal __.TaskId = taskId

[<Sealed;AbstractClass>]
type CloudArrayCache () =
    static let guid = System.Guid.NewGuid()

    // TODO : Replace
    static let createKey (ca : CachedCloudArray<'T>) pid = 
        sprintf "%s %s %d" ca.Id ca.TaskId pid
    static let parseKey (key : string) = 
        let key = key.Split()
        key.[0], key.[1], int key.[2]

    static let config = new System.Collections.Specialized.NameValueCollection()
    static do  config.Add("PhysicalMemoryLimitPercentage", "70")
    static let mc = new MemoryCache("CloudArrayMemoryCache", config)

    static let sync      = new obj()
    static let registry  = new HashSet<string * string * int>()
    static let occupied  = new HashSet<string>()
    static let policy    = new CacheItemPolicy()
    static do  policy.RemovedCallback <-
                new CacheEntryRemovedCallback(
                    fun args ->
                        lock sync (fun () -> registry.Remove(parseKey args.CacheItem.Key) |> ignore)
                )

    static member Guid = guid
    static member State = registry :> seq<_>
    static member Occupied = occupied :> seq<_>
        
    static member Add(ca : CachedCloudArray<'T>, pid : int, values : 'T []) =
        let key = createKey ca pid
        mc.Add(key, values, policy) |> ignore
        lock sync (fun () -> registry.Add(ca.Id, ca.TaskId, pid) |> ignore)
  
    static member Get(ca : CachedCloudArray<'T>, parentTaskId  : string) : seq<int> =
        lock sync (fun () -> 
            if occupied.Contains parentTaskId then
                Seq.empty
            else
                occupied.Add(parentTaskId) |> ignore
                registry 
                |> Seq.filter (fun (key, tid, _) -> key = ca.Id && ca.TaskId = tid )
                |> Seq.map (fun (_,_,s) -> s))

    static member GetPartition(ca : CachedCloudArray<'T>, pid : int) : 'T [] =
        let key = createKey ca pid
        mc.Get(key) :?> 'T []