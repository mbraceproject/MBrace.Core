namespace MBrace.Runtime

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

/// Collects statistics on CPU, memory, network, etc.
[<Sealed; AutoSerializable(false)>]
type PerformanceMonitor private (?updateInterval : int, ?maxSamplesCount : int, 
                                    ?cancellationToken : CancellationToken, ?logger : ISystemLogger) =

    // Get a new counter value after 0.1 sec and keep the last 10 values
    let updateInterval = defaultArg updateInterval 100
    let maxSamplesCount = defaultArg maxSamplesCount 10
    
    let cts = 
        match cancellationToken with
        | None -> new CancellationTokenSource()
        | Some tok -> CancellationTokenSource.CreateLinkedTokenSource(tok)

    let logger =
        match logger with
        | Some l -> l
        | None -> new NullLogger() :> _

    // computes a rolling average by asynchronously sampling data source
    // assumes values are all positive
    let mkAveragePoller (src : unit -> double) : (unit -> double) =
        let buf = Array.init maxSamplesCount (fun _ -> -1.)
        let rec poll i = async {
            buf.[i] <- try double(src()) with _ -> -1.
            do! Async.Sleep updateInterval
            return! poll ((i + 1) % buf.Length)
        }

        Async.Start(poll 0, cts.Token)

        fun () ->
            let mutable s, c = 0., 0
            for v in buf do
                if v >= 0. then c <- c + 1 ; s <- s + v

            if c = 0 then Double.NaN else s / float c

    static let mkPerfReader (pc : PerformanceCounter) =
        fun () -> pc.NextValue() |> double

    let perfCounters = new List<PerformanceCounter>()

    do if currentPlatform.Value = Platform.Windows then
        try ignore(PerformanceCounterCategory.Exists "a")
        with _ -> logger.Logf LogLevel.Warning "Error creating performance counters; corrupted counter cache. See also http://stackoverflow.com/a/18467730"

    static let categoryExistsSafe category = 
        try PerformanceCounterCategory.Exists category with _ -> false

    // Section: Performance counters 

    // Cpu Usage
    let getCpuUsage =
        match currentPlatform.Value with
        | Platform.OSX when currentRuntime.Value = Runtime.Mono ->
            // OSX/mono bug workaround https://bugzilla.xamarin.com/show_bug.cgi?id=41328
            let reader () =
                let _,results = runCommand ["ps"; "-A"; "-o" ; "%cpu"]
                let total =
                    results.Split([|Environment.NewLine|], StringSplitOptions.RemoveEmptyEntries) 
                    |> Array.sumBy (fun t -> let ok,d = Double.TryParse t in if ok then d else 0.)

                total

            try
                let _ = reader () in Some reader
            with e -> 
                logger.Logf LogLevel.Warning "Error generating CPU usage performance counter:%O" e
                None

        | _ when categoryExistsSafe "Processor" ->
            try
                let pc = new PerformanceCounter("Processor", "% Processor Time", "_Total", true)
                let reader = mkPerfReader pc
                let _ = reader ()
                perfCounters.Add pc
                reader |> mkAveragePoller |> Some
            with e ->
                logger.Logf LogLevel.Warning "Error generating CPU usage performance counter:%O" e
                None

        | _ -> None

    /// Max CPU frequency
    let getCpuFrequency =
        match currentPlatform.Value with
        | Platform.Windows ->
            try
                let getCpuClockSpeed () =
                    use searcher = new ManagementObjectSearcher("SELECT MaxClockSpeed FROM Win32_Processor")
                    use qObj = searcher.Get() 
                                |> Seq.cast<ManagementBaseObject> 
                                |> Seq.head

                    let cpuFreq = qObj.["MaxClockSpeed"] :?> uint32
                    double cpuFreq

                let _ = getCpuClockSpeed ()
                Some getCpuClockSpeed
            with e ->
                logger.Logf LogLevel.Warning "Error generating CPU frequency performance counter:%O" e
                None

        | Platform.Linux ->
            try
                let exitCode,results = runCommand ["lscpu"]
                if exitCode <> 0 then None else
                    let m = Regex.Match(results, "CPU max MHz:\s+([0-9\.]+)")
                    if m.Success then
                        let result = double m.Groups.[1].Value
                        Some(fun () -> result)
                    else
                        // docker images not reporting max MHz
                        let m = Regex.Match(results, "CPU MHz:\s+([0-9\.]+)")
                        if m.Success then
                            let result = double m.Groups.[1].Value
                            Some(fun () -> result)
                        else
                            None
            with e ->
                logger.Logf LogLevel.Warning "Error generating CPU frequency performance counter:%O" e
                None


        | Platform.OSX ->
            try
                let exitCode,results = runCommand ["sysctl"; "hw.cpufrequency"]
                if exitCode <> 0 then None else
                    let m = Regex.Match(results, "hw.cpufrequency: ([0-9]+)")
                    if m.Success then
                        let result = (float m.Groups.[1].Value) / 1e6
                        Some(fun () -> result)
                    else None
            with e ->
                logger.Logf LogLevel.Warning "Error generating CPU frequency performance counter:%O" e
                None

        | _ -> None
    
    let getTotalMemory = 
        match currentPlatform.Value with
        | Platform.Windows ->
            try
                use searcher = new ManagementObjectSearcher("root\\CIMV2", "SELECT TotalPhysicalMemory FROM Win32_ComputerSystem")
                use qObj = searcher.Get() 
                            |> Seq.cast<ManagementBaseObject> 
                            |> Seq.head
                let totalBytes = qObj.["TotalPhysicalMemory"] :?> uint64
                let mb = totalBytes / uint64 (1 <<< 20) |> double // size in MB
                Some(fun () -> mb)
            with e ->
                logger.Logf LogLevel.Warning "Error generating total memory performance counter:%O" e
                None

        | _ when categoryExistsSafe "Mono Memory" ->
            try
                let pc = new PerformanceCounter("Mono Memory", "Total Physical Memory")
                perfCounters.Add(pc)
                let totalBytes = pc.NextValue() |> uint64
                let mb = totalBytes / uint64 (1 <<< 20) |> double
                Some(fun () -> mb)
            with e ->
                logger.Logf LogLevel.Warning "Error generating total memory performance counter:%O" e
                None

        | _ -> None
    
    let getMemoryUsage =
        match currentPlatform.Value with
        | Platform.Windows when currentRuntime.Value = Runtime.DesktopCLR && 
                                categoryExistsSafe "Memory" && 
                                getTotalMemory.IsSome ->
            try
                let pc = new PerformanceCounter("Memory", "Available Mbytes", true)
                let reader () = getTotalMemory.Value() - double (pc.NextValue())
                let _ = reader ()
                perfCounters.Add pc
                Some reader
            with e ->
                logger.Logf LogLevel.Warning "Error generating memory usage performance counter:%O" e
                None
                

        | Platform.OSX ->
            let pageR = new Regex("page size of ([0-9]+) bytes", RegexOptions.Compiled)
            let entryR = new Regex("([\w ]+):\s+([0-9]+)\.", RegexOptions.Compiled)
            let reader () =
                let _,data = runCommand ["vm_stat"]
                let pageSize = pageR.Match(data).Groups.[1].Value |> uint64
                let d = new Dictionary<string, uint64>()
                for m in entryR.Matches(data) do d.Add(m.Groups.[1].Value, uint64 m.Groups.[2].Value)
                let usedBytes = pageSize * (d.["Pages active"] + d.["Pages wired down"])
                double (usedBytes / (1024uL * 1024uL))

            try let _ = reader () in Some reader
            with e ->
                logger.Logf LogLevel.Warning "Error generating memory usage performance counter:%O" e
                None

        | Platform.Linux ->
            let regex = new Regex("(\w+):\s+([0-9\.]+) kB", RegexOptions.Compiled)
            let reader () =
                let data = System.IO.File.ReadAllText("/proc/meminfo")
                let d = new Dictionary<string, uint64>()
                let matches = regex.Matches(data)
                for m in matches do d.Add(m.Groups.[1].Value, uint64 m.Groups.[2].Value)
                // formula from http://serverfault.com/q/442813
                let used = d.["MemTotal"] - d.["MemFree"] - d.["Buffers"] - d.["Cached"] - d.["SwapCached"]
                double (used / 1024uL)

            try let _ = reader () in Some reader
            with e ->
                logger.Logf LogLevel.Warning "Error generating memory usage performance counter:%O" e
                None

        | _ -> None
    

    // mono bug: https://bugzilla.xamarin.com/show_bug.cgi?id=41323
    let getNetworkSentUsage =
        if categoryExistsSafe "Network Interface" then
            try
                let category = new PerformanceCounterCategory("Network Interface")
                let pcs =
                    category.GetInstanceNames()
                    |> Array.map (fun nic -> new PerformanceCounter("Network Interface", "Bytes Sent/sec", nic))

                if pcs.Length = 0 then None else
                let reader () = pcs |> Array.sumBy (fun c -> double (c.NextValue()) / 1024.) // KB/s
                let _ = reader ()
                perfCounters.AddRange pcs
                Some reader
            with e ->
                logger.Logf LogLevel.Warning "Error generating network sent performance counter:%O" e
                None

        else None
    
    let getNetworkReceivedUsage =
        if categoryExistsSafe "Network Interface" then
            try
                let category = new PerformanceCounterCategory("Network Interface")
                let pcs =
                    category.GetInstanceNames()
                    |> Array.map (fun nic -> new PerformanceCounter("Network Interface", "Bytes Received/sec", nic))

                if pcs.Length = 0 then None else
                let reader () = pcs |> Array.sumBy (fun c -> double(c.NextValue()) / 1024.) // KB/s
                let _ = reader ()
                perfCounters.AddRange pcs
                Some reader
            with e ->
                logger.Logf LogLevel.Warning "Error generating network received performance counter:%O" e
                None

        else None

    // View information

    let monitored =
        [   if getCpuUsage.IsSome then  yield "%Cpu"
            if getCpuFrequency.IsSome then yield "Cpu Clock Speed"
            if getTotalMemory.IsSome then yield "Total Memory"
            if getMemoryUsage.IsSome then yield "Memory Used"
            if getNetworkSentUsage.IsSome then yield "Network (sent)"
            if getNetworkReceivedUsage.IsSome then yield "Network (received)" ]
    
    let getPerfValue (getterOpt : (unit -> double) option) : Nullable<double> =
        match getterOpt with
        | None -> Nullable()
        | Some getNext -> try Nullable(getNext()) with _ -> Nullable()

    let newNodePerformanceInfo () : PerformanceInfo =
        {
            CpuUsage            = getCpuUsage               |> getPerfValue
            MaxClockSpeed       = getCpuFrequency           |> getPerfValue
            TotalMemory         = getTotalMemory            |> getPerfValue
            MemoryUsage         = getMemoryUsage            |> getPerfValue
            NetworkUsageUp      = getNetworkSentUsage       |> getPerfValue
            NetworkUsageDown    = getNetworkReceivedUsage   |> getPerfValue
            Platform            = new Nullable<_>(currentPlatform.Value)
            Runtime             = new Nullable<_>(currentRuntime.Value)
        }

    /// <summary>
    ///     Starts a new performance monitor instance with supplied intervals.
    /// </summary>
    /// <param name="updateInterval">Polling interval in milliseconds. Defaults to 100.</param>
    /// <param name="maxSamplesCount">Max samples count. Defaults to 10.</param>
    /// <param name="cancellationToken">Cancellation token for the performance monitor.</param>
    /// <param name="logger">Logger used by the perf monitor.</param>
    static member Start(?updateInterval : int, ?maxSamplesCount : int, ?cancellationToken, ?logger) =
        new PerformanceMonitor(?updateInterval = updateInterval, ?maxSamplesCount = maxSamplesCount,
                                ?cancellationToken = cancellationToken, ?logger = logger)

    member this.GetCounters () : PerformanceInfo =
        if cts.IsCancellationRequested then 
            raise <| new ObjectDisposedException("PerformanceMonitor")

        newNodePerformanceInfo()

    member this.MonitoredCategories : string list = monitored

    interface System.IDisposable with
        member this.Dispose () = cts.Cancel()
