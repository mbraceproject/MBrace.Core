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

open MBrace.Core.Internals
open MBrace.Store
open MBrace.Store.Internals
open MBrace.Runtime.Utils
open MBrace.Runtime.Store
open MBrace.Runtime.Vagabond

type Config private () =

    static let isInitialized = ref false
    static let mutable workingDirectory = Unchecked.defaultof<string>
    static let mutable objectCache = Unchecked.defaultof<InMemoryCache>
    static let mutable _logger = Unchecked.defaultof<ICloudLogger>

    static let checkInitialized () =
        if not isInitialized.Value then
            invalidOp "Runtime configuration has not been initialized."

    static let init (workDir : string option) (createDir : bool option) (logger : ICloudLogger option) =
        if isInitialized.Value then invalidOp "Runtime configuration has already been initialized."
        let wd = match workDir with Some p -> p | None -> WorkingDirectory.GetDefaultWorkingDirectoryForProcess()
        let createDir = defaultArg createDir true
        let vagabondPath = Path.Combine(wd, "vagabond")

        let _ = System.Threading.ThreadPool.SetMinThreads(100, 100)

        objectCache <- InMemoryCache.Create()
        workingDirectory <- wd
        _logger <- match logger with Some l -> l | None -> new NullLogger() :> ICloudLogger

        // vagabond initialization
        VagabondRegistry.Initialize(cachePath = vagabondPath, cleanup = createDir, ignoredAssemblies = [Assembly.GetExecutingAssembly()], loadPolicy = AssemblyLoadPolicy.ResolveAll)

        // thespian initialization
        Nessos.Thespian.Serialization.defaultSerializer <- new FsPicklerMessageSerializer(VagabondRegistry.Instance.Serializer)
        Nessos.Thespian.Default.ReplyReceiveTimeout <- Timeout.Infinite
        TcpListenerPool.RegisterListener(IPEndPoint.any)
        isInitialized := true

    static member Init(?workDir : string, ?cleanup : bool, ?logger : ICloudLogger) = lock isInitialized (fun () -> init workDir cleanup logger)

    static member Serializer = checkInitialized() ; VagabondRegistry.Instance.Serializer
    static member WorkingDirectory = checkInitialized() ; workingDirectory
    static member ObjectCache = checkInitialized() ; objectCache :> IObjectCache
    static member LocalEndPoint = checkInitialized() ; TcpListenerPool.GetListener().LocalEndPoint
    static member LocalAddress = checkInitialized() ; sprintf "%s:%d" TcpListenerPool.DefaultHostname (TcpListenerPool.GetListener().LocalEndPoint.Port)

    static member Logger
        with get () = _logger
        and set l = _logger <- l


/// Actor publication utilities
type Actor =

    /// Publishes an actor instance to the default TCP protocol
    static member Publish(actor : Actor<'T>) =
        ignore Config.Serializer
        let name = Guid.NewGuid().ToString()
        actor
        |> Actor.rename name
        |> Actor.publish [ Protocols.utcp() ]
        |> Actor.start

    /// Exception-safe stateful actor behavior combinator
    static member Stateful (init : 'State) f = 
        let rec aux state (self : Actor<'T>) = async {
            let! msg = self.Receive()
            let! state' = async { 
                try return! f state msg 
                with e -> printfn "Actor fault (%O): %O" typeof<'T> e ; return state
            }

            return! aux state' self
        }

        Actor.bind (aux init)

    /// Exception-safe stateless actor behavior combinator
    static member Stateless (f : 'T -> Async<unit>) =
        Actor.Stateful () (fun () t -> f t)