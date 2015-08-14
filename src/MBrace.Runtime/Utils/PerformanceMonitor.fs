namespace MBrace.Runtime.Utils.PerformanceMonitor

open System
open System.Net
open System.Management
open System.Diagnostics
open System.Collections.Generic

open MBrace.Runtime.Utils

type private PerfCounter = System.Diagnostics.PerformanceCounter

/// Some node metrics, such as CPU, memory usage, etc
[<NoEquality; NoComparison>]
type PerformanceInfo =
    {
        /// CPU usage as percentage
        CpuUsage            : Nullable<double>
        /// Maximum CPU clock speed (in MHz)
        MaxClockSpeed       : Nullable<double>
        /// Total physical memory in machine
        TotalMemory         : Nullable<double>
        /// Total memory usage in machine
        MemoryUsage         : Nullable<double>
        /// Network upload in bytes
        NetworkUsageUp      : Nullable<double>
        /// Network download in bytes
        NetworkUsageDown    : Nullable<double>
    } 
with
    /// Creates an empty performance info object
    static member Empty =
        {
            CpuUsage = new Nullable<_>()
            MaxClockSpeed = new Nullable<_>()
            TotalMemory = new Nullable<_>()
            MemoryUsage = new Nullable<_>()
            NetworkUsageUp = new Nullable<_>()
            NetworkUsageDown = new Nullable<_>()
        }

type private Counter = TotalCpu | TotalMemoryUsage

[<NoEquality; NoComparison>]
type private Message = 
    | Info of AsyncReplyChannel<PerformanceInfo> 
    | Stop of AsyncReplyChannel<unit>

/// Collects statistics on CPU, memory, network, etc.
type PerformanceMonitor (?updateInterval : int, ?maxSamplesCount : int) =

    // Get a new counter value after 0.1 sec and keep the last 10 values
    let updateInterval = defaultArg updateInterval 100
    let maxSamplesCount = defaultArg maxSamplesCount 10
    
    let perfCounters = new List<PerfCounter>()

    // Performance counters 
    let cpuUsage =
        if PerformanceCounterCategory.Exists("Processor") then 
            let pc = new PerfCounter("Processor", "% Processor Time", "_Total",true)
            perfCounters.Add(pc)
            Some <| fun () -> pc.NextValue()
        else None

    let cpuFrequency =
        try
            let getCpuClockSpeed () =
                use searcher = new ManagementObjectSearcher("SELECT MaxClockSpeed FROM Win32_Processor")
                use qObj = searcher.Get() 
                            |> Seq.cast<ManagementBaseObject> 
                            |> Seq.exactlyOne

                let cpuFreq = qObj.["MaxClockSpeed"] :?> uint32
                single cpuFreq

            let _ = getCpuClockSpeed ()
            Some getCpuClockSpeed
        with _ -> None
    
    let totalMemory = 
        try
            use searcher = new ManagementObjectSearcher("root\\CIMV2", "SELECT TotalPhysicalMemory FROM Win32_ComputerSystem")
            use qObj = searcher.Get() 
                        |> Seq.cast<ManagementBaseObject> 
                        |> Seq.exactlyOne
            let totalBytes = qObj.["TotalPhysicalMemory"] :?> uint64
            let mb = totalBytes / uint64 (1 <<< 20) |> single // size in MB
            Some(fun () -> mb)
        with _ ->
            None
    
    let memoryUsage = 
        if PerformanceCounterCategory.Exists("Memory") && totalMemory.IsSome
        then 
            let pc = new PerfCounter("Memory", "Available Mbytes",true)
            perfCounters.Add(pc)
            Some <| (fun () -> totalMemory.Value() - pc.NextValue())
        else None
    
    let networkSentUsage =
        if PerformanceCounterCategory.Exists("Network Interface") then 
            let inst = (new PerformanceCounterCategory("Network Interface")).GetInstanceNames()
            let pc = 
                inst |> Array.map (fun nic -> new PerfCounter("Network Interface", "Bytes Sent/sec", nic))
            Seq.iter perfCounters.Add pc
            Some(fun () -> pc |> Array.fold (fun sAcc s -> sAcc + s.NextValue () / 1024.f) 0.f) // KB/s
        else None
    
    let networkReceivedUsage =
        if PerformanceCounterCategory.Exists("Network Interface") then 
            let inst = (new PerformanceCounterCategory("Network Interface")).GetInstanceNames()
            let pc = 
                inst |> Array.map (fun nic -> new PerfCounter("Network Interface", "Bytes Received/sec",nic))
            Seq.iter perfCounters.Add pc
            Some(fun () -> pc |> Array.fold (fun rAcc r -> rAcc + r.NextValue () / 1024.f ) 0.f) // KB/s
        else None
    
    let getPerfValue : (unit -> single) option -> Nullable<double> = function
        | None -> Nullable<_>()
        | Some(getNext) -> Nullable<_>(double <| getNext())
    
    let getAverage (values : Nullable<double> seq) =
        if values |> Seq.exists (fun v -> not v.HasValue) then Nullable<_>()
        else values |> Seq.map (function v -> v.Value)
                    |> Seq.average
                    |> fun v -> Nullable<_>(v)
    
    let cpuAvg = Queue<Nullable<double>>()
    
    let updateCpuQueue () =
        let newVal = getPerfValue cpuUsage
        if cpuAvg.Count < maxSamplesCount then cpuAvg.Enqueue newVal
        else cpuAvg.Dequeue() |> ignore; cpuAvg.Enqueue newVal
    
    let newNodePerformanceInfo () : PerformanceInfo =
        {
            CpuUsage            = cpuAvg                |> getAverage
            MaxClockSpeed       = cpuFrequency          |> getPerfValue
            TotalMemory         = totalMemory           |> getPerfValue
            MemoryUsage         = memoryUsage           |> getPerfValue
            NetworkUsageUp      = networkSentUsage      |> getPerfValue
            NetworkUsageDown    = networkReceivedUsage  |> getPerfValue
        }

    let perfCounterActor = 
        new MailboxProcessor<Message>(fun inbox ->    
            let rec agentLoop () : Async<unit> = async {
                updateCpuQueue ()
    
                while inbox.CurrentQueueLength <> 0 do
                    let! msg = inbox.Receive()
                    match msg with
                    | Stop ch -> ch.Reply (); return ()
                    | Info ch -> newNodePerformanceInfo () |> ch.Reply
    
                do! Async.Sleep updateInterval
    
                return! agentLoop ()
            }
            agentLoop ())

    let monitored =
        let l = new List<string>()
        if cpuUsage.IsSome then l.Add("%Cpu")
        if cpuFrequency.IsSome then l.Add("Cpu Clock Speed")
        if totalMemory.IsSome then l.Add("Total Memory")
        if memoryUsage.IsSome then l.Add("Memory Used")
        if networkSentUsage.IsSome then l.Add("Network (sent)")
        if networkReceivedUsage.IsSome then l.Add("Network (received)")
        l

    member this.GetCounters () : PerformanceInfo =
        perfCounterActor.PostAndReply(fun ch -> Info ch)

    member this.Start () =
        perfCounterActor.Start()
        this.GetCounters() |> ignore // first value always 0

    member this.MonitoredCategories : string seq = monitored :> _

    static member TryGetCpuClockSpeed () =
        if not runsOnMono then
            use searcher = new ManagementObjectSearcher("SELECT MaxClockSpeed FROM Win32_Processor")
            use qObj = searcher.Get() 
                        |> Seq.cast<ManagementBaseObject> 
                        |> Seq.exactlyOne

            let cpuFreq = qObj.["MaxClockSpeed"] :?> uint32
            Some <| float cpuFreq
        else
            None

    interface System.IDisposable with
        member this.Dispose () = 
            perfCounterActor.PostAndReply(fun ch -> Stop ch)
            perfCounters |> Seq.iter (fun c -> c.Dispose())  