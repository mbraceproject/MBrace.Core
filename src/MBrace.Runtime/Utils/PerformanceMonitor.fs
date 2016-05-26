namespace MBrace.Runtime.Utils.PerformanceMonitor

open System
open System.Net
open System.Management
open System.Diagnostics
open System.Collections.Generic
open System.Text.RegularExpressions
open System.Threading

open MBrace.Runtime.Utils

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
        /// Execution platform for current node
        Platform            : Nullable<Platform>
        /// Execution runtime for current node
        Runtime             : Nullable<Runtime>
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
            Platform = new Nullable<_>()
            Runtime = new Nullable<_>()
        }

type private RollingAverage(size : int) =
    let buf = Array.zeroCreate<float> size
    let mutable i = 0
    let mutable average = 0.
    member __.Enqueue(t : float) = buf.[i] <- t ; i <- (i + 1) % size ; average <- Array.average buf
    member __.Average = average

/// Collects statistics on CPU, memory, network, etc.
[<Sealed; AutoSerializable(false)>]
type PerformanceMonitor private (?updateInterval : int, ?maxSamplesCount : int, ?cancellationToken : CancellationToken) =
    // Get a new counter value after 0.1 sec and keep the last 10 values
    let updateInterval = defaultArg updateInterval 100
    let maxSamplesCount = defaultArg maxSamplesCount 10
    
    let cts = 
        match cancellationToken with
        | None -> new CancellationTokenSource()
        | Some tok -> CancellationTokenSource.CreateLinkedTokenSource(tok)

    let perfCounters = new List<PerformanceCounter>()

    // Section: Performance counters 

    // Cpu Usage
    let cpuUsage =
        match currentPlatform.Value with
        | Platform.OSX when currentRuntime.Value = Runtime.Mono ->
            // OSX/mono bug workaround https://bugzilla.xamarin.com/show_bug.cgi?id=41328
            None
            //let reader () =
            //    let _,results = runCommand "ps" "-A -o %cpu"
            //    let total =
            //        results.Split([|Environment.NewLine|], StringSplitOptions.RemoveEmptyEntries) 
            //        |> Array.sumBy (fun t -> let ok,d = Double.TryParse t in if ok then d else 0.)

            //    single total

            //Some reader

        | _ when PerformanceCounterCategory.Exists("Processor") ->
            let pc = new PerformanceCounter("Processor", "% Processor Time", "_Total", true)
            perfCounters.Add(pc)
            Some <| fun () -> pc.NextValue()

        | _ -> None

    // Average CPU usage polled over the past 1 second
    let cpuAvg = new RollingAverage(maxSamplesCount)
    let getCpuAvg =
        match cpuUsage with
        | None -> None
        | Some _ -> Some (fun () -> single cpuAvg.Average)

    /// Max CPU frequency
    let cpuFrequency =
        match currentPlatform.Value with
        | Platform.Windows ->
            try
                let getCpuClockSpeed () =
                    use searcher = new ManagementObjectSearcher("SELECT MaxClockSpeed FROM Win32_Processor")
                    use qObj = searcher.Get() 
                                |> Seq.cast<ManagementBaseObject> 
                                |> Seq.head

                    let cpuFreq = qObj.["MaxClockSpeed"] :?> uint32
                    single cpuFreq

                let _ = getCpuClockSpeed ()
                Some getCpuClockSpeed
            with _ -> None

        | Platform.Linux ->
            let exitCode,results = runCommand "lscpu" ""
            if exitCode <> 0 then None else
                let m = Regex.Match(results, "CPU max MHz:\s+([0-9\.]+)")
                if m.Success then
                    let result = single m.Groups.[1].Value
                    Some(fun () -> result)
                else None

        | Platform.OSX ->
            let exitCode,results = runCommand "sysctl" "hw.cpufrequency"
            if exitCode <> 0 then None else
                let m = Regex.Match(results, "hw.cpufrequency: ([0-9]+)")
                if m.Success then
                    let result = (float m.Groups.[1].Value) / 1e6 |> single
                    Some(fun () -> result)
                else None

        | _ -> None
    
    let totalMemory = 
        match currentPlatform.Value with
        | Platform.Windows ->
            try
                use searcher = new ManagementObjectSearcher("root\\CIMV2", "SELECT TotalPhysicalMemory FROM Win32_ComputerSystem")
                use qObj = searcher.Get() 
                            |> Seq.cast<ManagementBaseObject> 
                            |> Seq.head
                let totalBytes = qObj.["TotalPhysicalMemory"] :?> uint64
                let mb = totalBytes / uint64 (1 <<< 20) |> single // size in MB
                Some(fun () -> mb)
            with _ -> None

        | _ when PerformanceCounterCategory.Exists("Mono Memory") ->
            let pc = new PerformanceCounter("Mono Memory", "Total Physical Memory")
            perfCounters.Add(pc)
            let totalBytes = pc.NextValue() |> uint64
            let mb = totalBytes / uint64 (1 <<< 20) |> single
            Some(fun () -> mb)

        | _ -> None
    
    let memoryUsage =
        match currentPlatform.Value with
        | Platform.Windows when currentRuntime.Value = Runtime.DesktopCLR && 
                                PerformanceCounterCategory.Exists("Memory") && 
                                totalMemory.IsSome ->

            let pc = new PerformanceCounter("Memory", "Available Mbytes", true)
            perfCounters.Add(pc)
            Some <| (fun () -> totalMemory.Value() - pc.NextValue())
                

        | Platform.OSX ->
            let pageR = new Regex("page size of ([0-9]+) bytes", RegexOptions.Compiled)
            let entryR = new Regex("([\w ]+):\s+([0-9]+)\.", RegexOptions.Compiled)
            let reader () =
                let _,data = runCommand "vm_stat" ""
                let pageSize = pageR.Match(data).Groups.[1].Value |> uint64
                let d = new Dictionary<string, uint64>()
                for m in entryR.Matches(data) do d.Add(m.Groups.[1].Value, uint64 m.Groups.[2].Value)
                let usedBytes = pageSize * (d.["Pages active"] + d.["Pages wired down"])
                single (usedBytes / (1024uL * 1024uL))

            Some reader

        | Platform.Linux ->
            let regex = new Regex("(\w+):\s+([0-9\.]+) kB", RegexOptions.Compiled)
            let reader () =
                let data = System.IO.File.ReadAllText("/proc/meminfo")
                let d = new Dictionary<string, uint64>()
                let matches = regex.Matches(data)
                for m in matches do d.Add(m.Groups.[1].Value, uint64 m.Groups.[2].Value)
                // formula from http://serverfault.com/q/442813
                let used = d.["MemTotal"] - d.["MemFree"] - d.["Buffers"] - d.["Cached"] - d.["SwapCached"]
                single (used / 1024uL)

            Some reader

        | _ -> None
    

    // mono bug: https://bugzilla.xamarin.com/show_bug.cgi?id=41323
    let networkSentUsage =
        if PerformanceCounterCategory.Exists("Network Interface") then
            let category = new PerformanceCounterCategory("Network Interface")
            let pcs =
                category.GetInstanceNames()
                |> Array.map (fun nic -> new PerformanceCounter("Network Interface", "Bytes Sent/sec", nic))

            if pcs.Length = 0 then None else
            perfCounters.AddRange pcs
            Some(fun () -> pcs |> Array.sumBy (fun c -> c.NextValue () / 1024.f)) // KB/s

        else None
    
    let networkReceivedUsage =
        if PerformanceCounterCategory.Exists("Network Interface") then
            let category = new PerformanceCounterCategory("Network Interface")
            let pcs =
                category.GetInstanceNames()
                |> Array.map (fun nic -> new PerformanceCounter("Network Interface", "Bytes Received/sec", nic))

            if pcs.Length = 0 then None else
            perfCounters.AddRange pcs
            Some(fun () -> pcs |> Array.sumBy (fun c -> c.NextValue () / 1024.f)) // KB/s

        else None

    // View information

    let monitored =
        [   if cpuUsage.IsSome then  yield "%Cpu"
            if cpuFrequency.IsSome then yield "Cpu Clock Speed"
            if totalMemory.IsSome then yield "Total Memory"
            if memoryUsage.IsSome then yield "Memory Used"
            if networkSentUsage.IsSome then yield "Network (sent)"
            if networkReceivedUsage.IsSome then yield "Network (received)" ]
    
    let getPerfValue (getterOpt : (unit -> single) option) : Nullable<double> =
        match getterOpt with
        | None -> Nullable<_>()
        | Some getNext -> Nullable<_>(double <| getNext())

    let newNodePerformanceInfo () : PerformanceInfo =
        {
            CpuUsage            = getCpuAvg             |> getPerfValue
            MaxClockSpeed       = cpuFrequency          |> getPerfValue
            TotalMemory         = totalMemory           |> getPerfValue
            MemoryUsage         = memoryUsage           |> getPerfValue
            NetworkUsageUp      = networkSentUsage      |> getPerfValue
            NetworkUsageDown    = networkReceivedUsage  |> getPerfValue
            Platform            = new Nullable<_>(currentPlatform.Value)
            Runtime             = new Nullable<_>(currentRuntime.Value)
        }

    // polling loop behaviour
    let rec poller () = async {
        try 
            // Add polling operations here
            cpuUsage |> Option.iter (fun f -> f () |> double |> cpuAvg.Enqueue)

        with _ -> ()

        do! Async.Sleep updateInterval
        return! poller ()
    }

    do Async.Start(poller(), cts.Token)

    /// <summary>
    ///     Starts a new performance monitor instance with supplied intervals.
    /// </summary>
    /// <param name="updateInterval">Polling interval in milliseconds. Defaults to 100.</param>
    /// <param name="maxSamplesCount">Max samples count. Defaults to 10.</param>
    /// <param name="cancellationToken">Cancellation token for the performance monitor.</param>
    static member Start(?updateInterval : int, ?maxSamplesCount : int, ?cancellationToken) =
        new PerformanceMonitor(?updateInterval = updateInterval, ?maxSamplesCount = maxSamplesCount,
                                ?cancellationToken = cancellationToken)

    member this.GetCounters () : PerformanceInfo =
        if cts.IsCancellationRequested then 
            raise <| new ObjectDisposedException("PerformanceMonitor")

        newNodePerformanceInfo()

    member this.MonitoredCategories : string seq = monitored :> _

    interface System.IDisposable with
        member this.Dispose () = cts.Cancel()