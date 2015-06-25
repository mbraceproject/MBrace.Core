namespace MBrace.Thespian.Runtime

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
    static let mutable objectCache = Unchecked.defaultof<InMemoryCache>
    static let mutable workingDirectory = Unchecked.defaultof<string>

    static let initVagabond populateDirs (path:string) =
        if populateDirs then ignore <| Directory.CreateDirectory path
        let policy = AssemblyLookupPolicy.ResolveRuntimeStrongNames ||| AssemblyLookupPolicy.ResolveVagabondCache
        Vagabond.Initialize(ignoredAssemblies = [Assembly.GetExecutingAssembly()], cacheDirectory = path, lookupPolicy = policy)

    static let checkInitialized () =
        if not isInitialized.Value then
            invalidOp "Runtime configuration has not been initialized."

    static let init (populateDirs : bool) =
        if isInitialized.Value then invalidOp "Runtime configuration has already been initialized."
        workingDirectory <- WorkingDirectory.CreateWorkingDirectory(cleanup = populateDirs)
        let vagabondDir = Path.Combine(workingDirectory, "vagabond")
        VagabondRegistry.Initialize(fun () -> initVagabond populateDirs vagabondDir)

        let _ = System.Threading.ThreadPool.SetMinThreads(100, 100)
        objectCache <- InMemoryCache.Create()

        // thespian initialization
        Nessos.Thespian.Serialization.defaultSerializer <- new FsPicklerMessageSerializer(VagabondRegistry.Instance.Serializer)
        Nessos.Thespian.Default.ReplyReceiveTimeout <- Timeout.Infinite
        TcpListenerPool.RegisterListener(IPEndPoint.any)
        isInitialized := true


    static member Init(?populateDirs : bool) = lock isInitialized (fun () -> init (defaultArg populateDirs true))

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