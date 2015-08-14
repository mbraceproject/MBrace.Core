namespace MBrace.Thespian.Runtime

open System
open System.IO
open System.Net
open System.Reflection
open System.Threading

open Nessos.Vagabond

open Nessos.Thespian
open Nessos.Thespian.Serialization
open Nessos.Thespian.Remote
open Nessos.Thespian.Remote.TcpProtocol
open Nessos.Thespian.DebugUtils

open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.Store
open MBrace.Runtime.Vagabond

/// MBrace.Thespian configuration object
type Config private () =

    static let _isInitialized = ref false
    static let mutable _objectCache = Unchecked.defaultof<InMemoryCache>
    static let mutable _localFileStore = Unchecked.defaultof<FileSystemStore>
    static let mutable _workingDirectory = Unchecked.defaultof<string>
    static let mutable _localTcpPort = Unchecked.defaultof<int>

    static let initVagabond populateDirs (path:string) =
        if populateDirs then ignore <| Directory.CreateDirectory path
        let policy = AssemblyLookupPolicy.ResolveRuntimeStrongNames ||| AssemblyLookupPolicy.ResolveVagabondCache
        Vagabond.Initialize(ignoredAssemblies = [Assembly.GetExecutingAssembly()], cacheDirectory = path, lookupPolicy = policy)

    static let checkInitialized () =
        if not _isInitialized.Value then
            invalidOp "Runtime configuration has not been initialized."

    /// <summary>
    ///     Initializes the global configuration object.
    /// </summary>
    /// <param name="populateDirs">Create or clear working directory.</param>
    static member Initialize(populateDirs : bool, ?hostname : string, ?workingDirectory : string, ?port : int) =
        lock _isInitialized (fun () ->
            if _isInitialized.Value then invalidOp "Runtime configuration has already been initialized."
            _workingDirectory <- WorkingDirectory.CreateWorkingDirectory(?path = workingDirectory ,cleanup = populateDirs)
            let vagabondDir = Path.Combine(_workingDirectory, "vagabond")
            VagabondRegistry.Initialize(fun () -> initVagabond populateDirs vagabondDir)

            let _ = System.Threading.ThreadPool.SetMinThreads(100, 100)
            _objectCache <- InMemoryCache.Create()

            let fsStoreDirectory = Path.Combine(_workingDirectory, "store")
            _localFileStore <- FileSystemStore.Create(fsStoreDirectory, create = true, cleanup = populateDirs)

            // thespian initialization
            let thespianSerializer = new FsPicklerMessageSerializer(VagabondRegistry.Instance.Serializer)

//            let thespianSerializer = new DebugSerializer(thespianSerializer) // use for debug information on serialized values
            Nessos.Thespian.Serialization.defaultSerializer <- thespianSerializer
            Nessos.Thespian.Default.ReplyReceiveTimeout <- Timeout.Infinite
            hostname |> Option.iter (fun h -> TcpListenerPool.DefaultHostname <- h ; ignore TcpListenerPool.IPAddresses)
            TcpListenerPool.RegisterListener(defaultArg port 0)
            _localTcpPort <- TcpListenerPool.GetListener().LocalEndPoint.Port
            _isInitialized := true)

    /// FsPickler serializer instance used by MBrace.Thespian
    static member Serializer = checkInitialized() ; VagabondRegistry.Instance.Serializer
    /// Working directory used by the instance
    static member WorkingDirectory = checkInitialized() ; _workingDirectory
    /// Local FileSystemStore instance
    static member FileSystemStore = checkInitialized() ; _localFileStore
    /// Object Cache used by the instance
    static member ObjectCache = checkInitialized() ; _objectCache
    /// TCP Port used by the local Thespian instance
    static member LocalTcpPort = checkInitialized() ; _localTcpPort
    /// Local TCP address used by the local Thespian instance
    static member LocalAddress = checkInitialized() ; sprintf "%s:%d" TcpListenerPool.DefaultHostname _localTcpPort