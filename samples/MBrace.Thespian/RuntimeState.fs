namespace MBrace.Thespian.Runtime

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Store.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.Vagabond

type RuntimeId = private { Id : string }
with
    interface IRuntimeId with
        member x.Id: string = x.Id

    override x.ToString() = x.Id

    static member Create() = { Id = mkUUID() }
    
    
[<NoEquality; NoComparison>]
type RuntimeState =
    {
        Id : RuntimeId
        Address : string

        Factory : ResourceFactory
        WorkerManager : WorkerManager
        CloudLogger  : ActorCloudLogger
        JobQueue : IJobQueue
        Resources : ResourceRegistry
        AssemblyDirectory : string
        TaskManager : CloudTaskManager
    }
with
    static member InitLocal(localLogger : ISystemLogger, fileStoreConfig : CloudFileStoreConfiguration, 
                                ?assemblyDirectory : string, ?miscResources : ResourceRegistry) =

        let wmon = WorkerManager.Init()
        let factory = ResourceFactory.Create()
        let assemblyDirectory = defaultArg assemblyDirectory "vagabond"
        let resources = resource {
            yield CloudAtomConfiguration.Create(new ActorAtomProvider(factory))
            yield CloudChannelConfiguration.Create(new ActorChannelProvider(factory))
            yield new ActorDictionaryProvider(factory) :> ICloudDictionaryProvider
            yield new FsPicklerBinaryStoreSerializer() :> ISerializer
            match miscResources with Some r -> yield! r | None -> ()
            yield fileStoreConfig
        }

        {
            Id = RuntimeId.Create()
            Address = Config.LocalAddress

            Factory = factory
            WorkerManager = wmon
            TaskManager = CloudTaskManager.Init()
            CloudLogger = ActorCloudLogger.Create(localLogger)
            JobQueue = JobQueue.Init(wmon)
            Resources = resources
            AssemblyDirectory = assemblyDirectory
        }

    member state.ToBase64 () =
        let pickle = Config.Serializer.Pickle(state)
        System.Convert.ToBase64String pickle

    static member FromBase64(base64Text:string) =
        let bytes = System.Convert.FromBase64String(base64Text)
        Config.Serializer.UnPickle<RuntimeState> bytes