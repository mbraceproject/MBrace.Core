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

    static let isInitialized = ref false
    static let mutable objectCache = Unchecked.defaultof<InMemoryCache>
    static let mutable localFileStore = Unchecked.defaultof<FileSystemStore>
    static let mutable workingDirectory = Unchecked.defaultof<string>
    static let mutable localEndpoint = Unchecked.defaultof<IPEndPoint>

    static let initVagabond populateDirs (path:string) =
        if populateDirs then ignore <| Directory.CreateDirectory path
        let policy = AssemblyLookupPolicy.ResolveRuntimeStrongNames ||| AssemblyLookupPolicy.ResolveVagabondCache
        Vagabond.Initialize(ignoredAssemblies = [Assembly.GetExecutingAssembly()], cacheDirectory = path, lookupPolicy = policy)

    static let checkInitialized () =
        if not isInitialized.Value then
            invalidOp "Runtime configuration has not been initialized."

    /// <summary>
    ///     Initializes the global configuration object.
    /// </summary>
    /// <param name="populateDirs">Create or clear working directory.</param>
    static member Initialize(populateDirs : bool) =
        lock isInitialized (fun () ->
            if isInitialized.Value then invalidOp "Runtime configuration has already been initialized."
            workingDirectory <- WorkingDirectory.CreateWorkingDirectory(cleanup = populateDirs)
            let vagabondDir = Path.Combine(workingDirectory, "vagabond")
            VagabondRegistry.Initialize(fun () -> initVagabond populateDirs vagabondDir)

            let _ = System.Threading.ThreadPool.SetMinThreads(100, 100)
            objectCache <- InMemoryCache.Create()

            let fsStoreDirectory = Path.Combine(workingDirectory, "store")
            localFileStore <- FileSystemStore.Create(fsStoreDirectory, create = true, cleanup = populateDirs)

            // thespian initialization
            let thespianSerializer = new FsPicklerMessageSerializer(VagabondRegistry.Instance.Serializer)
//            let thespianSerializer = new DebugSerializer(thespianSerializer) // use for debug information on serialized values
            Nessos.Thespian.Serialization.defaultSerializer <- thespianSerializer
            Nessos.Thespian.Default.ReplyReceiveTimeout <- Timeout.Infinite
            TcpListenerPool.RegisterListener(IPEndPoint.any)
            localEndpoint <- TcpListenerPool.GetListener().LocalEndPoint
            isInitialized := true)

    /// FsPickler serializer instance used by MBrace.Thespian
    static member Serializer = checkInitialized() ; VagabondRegistry.Instance.Serializer
    /// Working directory used by the instance
    static member WorkingDirectory = checkInitialized() ; workingDirectory
    /// Local FileSystemStore instance
    static member FileSystemStore = checkInitialized() ; localFileStore
    /// Object Cache used by the instance
    static member ObjectCache = checkInitialized() ; objectCache
    /// TCP Endpoing used by the local Thespian instance
    static member LocalEndPoint = checkInitialized() ; localEndpoint
    /// Local TCP address used by the local Thespian instance
    static member LocalAddress = checkInitialized() ; sprintf "%s:%d" TcpListenerPool.DefaultHostname localEndpoint.Port