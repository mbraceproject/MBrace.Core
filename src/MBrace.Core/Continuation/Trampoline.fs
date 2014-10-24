namespace Nessos.MBrace.Runtime

    open System
    open System.Threading

    type Trampoline private () =

        static let runsOnMono = System.Type.GetType("Mono.Runtime") <> null
        static let isCLR40OrLater = System.Environment.Version.Major >= 4
        static let isTrampolineEnabled = runsOnMono || not isCLR40OrLater

        static let instance = new Threading.ThreadLocal<_>(fun () -> new Trampoline())
        static let threshold = 200
        let mutable bindCount = 0
        member __.IsBindThresholdReached () = 
            bindCount <- bindCount + 1
            bindCount > threshold

        member __.QueueWorkItem cb = 
            bindCount <- 0
            if not <| ThreadPool.QueueUserWorkItem cb then
                invalidOp "internal error: could not queue work item to thread pool."

        static member LocalInstance = instance.Value
        static member IsTrampolineEnabled = isTrampolineEnabled