module internal Nessos.MBrace.SampleRuntime.Config

open System
open System.Reflection
open System.Threading

open Nessos.Thespian
open Nessos.Thespian.Serialization
open Nessos.Thespian.Remote
open Nessos.Thespian.Remote.TcpProtocol

open Nessos.Vagrant

open Nessos.MBrace.Runtime

let private runOnce (f : unit -> 'T) = let v = lazy(f ()) in fun () -> v.Value

// vagrant, fspickler and thespian state initializations
let private _initRuntimeState () =
    let _ = System.Threading.ThreadPool.SetMinThreads(100, 100)

    // vagrant initialization
    let ignoredAssemblies =
        let this = Assembly.GetExecutingAssembly()
        let dependencies = Utilities.ComputeAssemblyDependencies(this, requireLoadedInAppDomain = false)
        new System.Collections.Generic.HashSet<_>(dependencies)

    VagrantRegistry.Initialize(ignoreAssembly = ignoredAssemblies.Contains, loadPolicy = AssemblyLoadPolicy.ResolveAll)

    // thespian initialization
    Nessos.Thespian.Serialization.defaultSerializer <- new FsPicklerMessageSerializer(VagrantRegistry.Pickler)
    Nessos.Thespian.Default.ReplyReceiveTimeout <- Timeout.Infinite
    TcpListenerPool.RegisterListener(IPEndPoint.any)

let initRuntimeState = runOnce _initRuntimeState
let getLocalEndpoint () = initRuntimeState () ; TcpListenerPool.GetListener().LocalEndPoint
