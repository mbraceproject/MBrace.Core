namespace MBrace.SampleRuntime

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Store.Internals
open MBrace.Runtime
open MBrace.Runtime.Vagabond

type RuntimeState =
    {
        Id : string
        Address : string

        Factory : ResourceFactory
        WorkerMonitor : WorkerMonitor
        CloudLogger  : ActorCloudLogger
        JobQueue : IJobQueue
        Resources : ResourceRegistry
        AssemblyDirectory : string
        TaskManager : CloudTaskManager
    }
with
    static member InitLocal(localLogger : ISystemLogger, fileStoreConfig : CloudFileStoreConfiguration, 
                                ?assemblyDirectory : string, ?miscResources : ResourceRegistry) =

        let wmon = WorkerMonitor.Init()
        let factory = ResourceFactory.Init()
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
            Id = mkUUID()
            Address = Config.LocalAddress

            Factory = factory
            WorkerMonitor = wmon
            TaskManager = CloudTaskManager.Init()
            CloudLogger = ActorCloudLogger.Init(localLogger)
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