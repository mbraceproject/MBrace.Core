namespace MBrace.SampleRuntime

open System
open System.IO
open System.Reflection
open System.Threading

open Nessos.Vagabond

open Nessos.Thespian
open Nessos.Thespian.Serialization
open Nessos.Thespian.Remote
open Nessos.Thespian.Remote.TcpProtocol

open MBrace.Store
open MBrace.Runtime.Utils
open MBrace.Runtime.Store
open MBrace.Runtime.Vagabond

type Config private () =

    static let isInitialized = ref false
    static let mutable workingDirectory = Unchecked.defaultof<string>
    static let mutable objectCache = Unchecked.defaultof<InMemoryCache>
    static let mutable fileCache = Unchecked.defaultof<FileSystemStore>

    static let checkInitialized () =
        if not isInitialized.Value then
            invalidOp "Runtime configuration has not been initialized."

    static let init (workDir : string option) (createDir : bool option) =
        if isInitialized.Value then invalidOp "Runtime configuration has already been initialized."
        let wd = match workDir with Some p -> p | None -> WorkingDirectory.GetDefaultWorkingDirectoryForProcess()
        let createDir = defaultArg createDir true
        let vagrantPath = Path.Combine(wd, "vagrant")
        let fileCachePath = Path.Combine(wd, "fileCache")

        let _ = System.Threading.ThreadPool.SetMinThreads(100, 100)

        objectCache <- InMemoryCache.Create()
        fileCache <- FileSystemStore.Create(fileCachePath, create = createDir, cleanup = createDir)
        workingDirectory <- wd

        // vagabond initialization
        VagabondRegistry.Initialize(cachePath = vagrantPath, cleanup = createDir, ignoredAssemblies = [Assembly.GetExecutingAssembly()], loadPolicy = AssemblyLoadPolicy.ResolveAll)

        // thespian initialization
        Nessos.Thespian.Serialization.defaultSerializer <- new FsPicklerMessageSerializer(VagabondRegistry.Instance.Pickler)
        Nessos.Thespian.Default.ReplyReceiveTimeout <- Timeout.Infinite
        TcpListenerPool.RegisterListener(IPEndPoint.any)
        isInitialized := true

    static member Init(?workDir : string, ?cleanup : bool) = lock isInitialized (fun () -> init workDir cleanup)

    static member Pickler = checkInitialized() ; VagabondRegistry.Instance.Pickler
    static member WorkingDirectory = checkInitialized() ; workingDirectory
    static member FileStoreCache = checkInitialized() ; fileCache
    static member ObjectCache = checkInitialized() ; objectCache :> IObjectCache
    static member LocalEndPoint = checkInitialized() ; TcpListenerPool.GetListener().LocalEndPoint
    static member LocalAddress = 
        checkInitialized() ; sprintf "%s:%d" TcpListenerPool.DefaultHostname (TcpListenerPool.GetListener().LocalEndPoint.Port)

    static member WithCachedFileStore(config : CloudFileStoreConfiguration) =
        let cacheStore = FileStoreCache.Create(config.FileStore, Config.FileStoreCache, localCacheContainer = "cache") :> ICloudFileStore
        { config with FileStore = cacheStore }