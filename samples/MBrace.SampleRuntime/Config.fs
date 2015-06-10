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
open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.Store
open MBrace.Runtime.Vagabond

type Config private () =

    static let isInitialized = ref false
    static let mutable workingDirectory = Unchecked.defaultof<string>
    static let mutable objectCache = Unchecked.defaultof<InMemoryCache>

    static let checkInitialized () =
        if not isInitialized.Value then
            invalidOp "Runtime configuration has not been initialized."

    static let init (workDir : string option) (createDir : bool option) =
        if isInitialized.Value then invalidOp "Runtime configuration has already been initialized."
        let wd = match workDir with Some p -> p | None -> WorkingDirectory.GetDefaultWorkingDirectoryForProcess()
        let createDir = defaultArg createDir true
        let vagabondDir = Path.Combine(wd, "vagabond")

        let _ = System.Threading.ThreadPool.SetMinThreads(100, 100)

        objectCache <- InMemoryCache.Create()
        workingDirectory <- wd

        // vagabond initialization
        VagabondRegistry.Initialize(cachePath = vagabondDir, cleanup = createDir, ignoredAssemblies = [Assembly.GetExecutingAssembly()], lookupPolicy = (AssemblyLookupPolicy.ResolveRuntime ||| AssemblyLookupPolicy.ResolveVagabondCache))

        // thespian initialization
        Nessos.Thespian.Serialization.defaultSerializer <- new FsPicklerMessageSerializer(VagabondRegistry.Instance.Serializer)
        Nessos.Thespian.Default.ReplyReceiveTimeout <- Timeout.Infinite
        TcpListenerPool.RegisterListener(IPEndPoint.any)
        isInitialized := true

    static member Init(?workDir : string, ?cleanup : bool) = lock isInitialized (fun () -> init workDir cleanup)

    static member Serializer = checkInitialized() ; VagabondRegistry.Instance.Serializer
    static member WorkingDirectory = checkInitialized() ; workingDirectory
    static member ObjectCache = checkInitialized() ; objectCache :> IObjectCache
    static member LocalEndPoint = checkInitialized() ; TcpListenerPool.GetListener().LocalEndPoint
    static member LocalAddress = checkInitialized() ; sprintf "%s:%d" TcpListenerPool.DefaultHostname (TcpListenerPool.GetListener().LocalEndPoint.Port)

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

    /// <summary>
    ///     Stateful actor behaviour combinator passed self actor.
    ///     Catches behaviour exceptions and retains original state.
    /// </summary>
    /// <param name="init">Initial state.</param>
    /// <param name="behaviour">Actor body behaviour.</param>
    static member SelfStateful (init : 'State) (behaviour : Actor<'T> -> 'State -> 'T -> Async<'State>) : Actor<'T> = 
        let rec aux state (self : Actor<'T>) = async {
            let! msg = self.Receive()
            let! state' = async { 
                try return! behaviour self state msg 
                with e -> printfn "Actor fault (%O): %O" typeof<'T> e ; return state
            }

            return! aux state' self
        }

        Actor.bind (aux init)

    /// <summary>
    ///     Stateful actor behaviour combinator.
    ///     Catches behaviour exceptions and retains original state.
    /// </summary>
    /// <param name="init">Initial state.</param>
    /// <param name="behaviour">Actor body behaviour.</param>
    static member Stateful (init : 'State) (behaviour : 'State -> 'T -> Async<'State>) : Actor<'T> = 
        let rec aux state (self : Actor<'T>) = async {
            let! msg = self.Receive()
            let! state' = async { 
                try return! behaviour state msg 
                with e -> printfn "Actor fault (%O): %O" typeof<'T> e ; return state
            }

            return! aux state' self
        }

        Actor.bind (aux init)

    /// <summary>
    ///     Stateless actor behaviour combinator.
    ///     Catches behaviour exceptions and retains original state.
    /// </summary>
    /// <param name="init">Initial state.</param>
    /// <param name="behaviour">Actor body behaviour.</param>
    static member Stateless (behaviour : 'T -> Async<unit>) : Actor<'T>=
        let rec aux (self : Actor<'T>) = async {
            let! msg = self.Receive()
            try do! behaviour msg
            with e -> printfn "Actor fault (%O): %O" typeof<'T> e 
            return! aux self
        }

        Actor.bind aux