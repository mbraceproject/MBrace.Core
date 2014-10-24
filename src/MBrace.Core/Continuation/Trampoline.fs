namespace Nessos.MBrace

    open System
    open System.Threading
    open System.ComponentModel

    type internal Trampoline private () =

        static let runsOnMono = System.Type.GetType("Mono.Runtime") <> null
        static let isCLR40OrLater = System.Environment.Version.Major >= 4
        // runtimes that support tail call optimization do not need trampoline support.
        static let isTrampolineEnabled = runsOnMono || not isCLR40OrLater

        static let threadInstance = new Threading.ThreadLocal<_>(fun () -> new Trampoline())
        
        [<Literal>]
        static let threshold = 200
        let mutable bindCount = 0

        member __.IsBindThresholdReached () =
            if bindCount + 1 > threshold then
                bindCount <- 0
                true
            else
                bindCount <- bindCount + 1
                false

        member __.Reset () = bindCount <- 0

        static member IsBindThresholdReached () =
            if isTrampolineEnabled then
                threadInstance.Value.IsBindThresholdReached()
            else
                false

        static member Reset () =
            if isTrampolineEnabled then
                threadInstance.Value.Reset()

        static member QueueWorkItem (f : unit -> unit) : unit =
            let cb = new WaitCallback(fun _ -> threadInstance.Value.Reset() ; f ())
            if not <| ThreadPool.QueueUserWorkItem cb then
                invalidOp "internal error: could not queue work item to thread pool."