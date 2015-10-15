namespace MBrace.Core.Internals

open System
open System.Threading

/// Mechanism for offloading execution stack in the thread pool
type internal Trampoline private () =

    static let isTailCallOptimizingRuntime = 
#if DEBUG
        false // tail calls are not generated in debug builds, so assume that this is true
#else
        let runsOnMono = System.Type.GetType("Mono.Runtime") <> null
        let isCLR40OrLater = System.Environment.Version.Major >= 4
        not runsOnMono && isCLR40OrLater
#endif

    static let threadInstance = new ThreadLocal<_>(fun () -> new Trampoline())
    static let [<Literal>] bindThreshold = 300
    let mutable bindCount = 0  // number of continuation bindings performed in current thread.

    member private __.IsBindThresholdReached () =
        let bc = bindCount + 1
        if bc > bindThreshold then
            bindCount <- 0
            true
        else
            bindCount <- bc
            false

    member private __.Reset () = bindCount <- 0

    /// Checks if continuation execution stack has reached specified threshold in the current thread.
    static member IsBindThresholdReached (isContinuationExposed : bool) =
        if isContinuationExposed || not isTailCallOptimizingRuntime then
            threadInstance.Value.IsBindThresholdReached()
        else
            false

    /// Queue a new work item to the .NET thread pool.
    static member QueueWorkItem (f : unit -> unit) : unit =
        let cb = new WaitCallback(fun _ -> threadInstance.Value.Reset() ; f ())
        if not <| ThreadPool.QueueUserWorkItem cb then
            invalidOp "internal error: could not queue work item to thread pool."