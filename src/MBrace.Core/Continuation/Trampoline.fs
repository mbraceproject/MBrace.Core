namespace Nessos.MBrace.Continuation

    open System
    open System.Threading

    /// Mechanism for offloading execution stack in the thread pool
    type internal Trampoline private () =

        static let runsOnMono = System.Type.GetType("Mono.Runtime") <> null
        static let isCLR40OrLater = System.Environment.Version.Major >= 4
        // runtimes that support tail call optimization do not require trampoline support.
        static let isTrampolineEnabled = runsOnMono || not isCLR40OrLater

        static let threadInstance = new Threading.ThreadLocal<_>(fun () -> new Trampoline())
        
        [<Literal>]
        static let threshold = 300 // schedule continuation to new thread, if threshold is reached.
        let mutable bindCount = 0  // number of continuation bindings performed in current thread.

        /// Checks if continuation execution stack has reached specified threshold in the current thread.
        member private __.IsBindThresholdReached () =
            if bindCount + 1 > threshold then
                bindCount <- 0
                true
            else
                bindCount <- bindCount + 1
                false

        /// Resets the trampoline state in the current thread.
        member private __.Reset () = bindCount <- 0

        /// Checks if continuation execution stack has reached specified threshold in the current thread.
        static member IsBindThresholdReached () =
            if isTrampolineEnabled then
                threadInstance.Value.IsBindThresholdReached()
            else
                false

        /// Resets the trampoline state in the current thread.
        static member Reset () =
            if isTrampolineEnabled then
                threadInstance.Value.Reset()

        /// Queue a new work item to the .NET thread pool.
        static member QueueWorkItem (f : unit -> unit) : unit =
            let cb = new WaitCallback(fun _ -> Trampoline.Reset() ; f ())
            if not <| ThreadPool.QueueUserWorkItem cb then
                invalidOp "internal error: could not queue work item to thread pool."