namespace MBrace.Runtime

open System

//#nowarn "52"

open MBrace.Core
open MBrace.Runtime.Utils.PrettyPrinters



//type internal WorkerReporter() = 
//    static let template : Field<IWorkerRef> list = 
//        let double_printer (value : double) = 
//            if value < 0. then "N/A" else sprintf "%.1f" value
//        [ Field.create "Id" Left (fun p -> p.Id)
//          Field.create "Status" Left (fun p -> string p.Status)
//          Field.create "% CPU / Cores" Center (fun p -> sprintf "%s / %d" (double_printer p.CPU) p.ProcessorCount)
//          Field.create "% Memory / Total(MB)" Center (fun p -> 
//            let memPerc = 100. * p.Memory / p.TotalMemory |> double_printer
//            sprintf "%s / %s" memPerc <| double_printer p.TotalMemory)
//          Field.create "Network(ul/dl : KB/s)" Center (fun n -> sprintf "%s / %s" <| double_printer n.NetworkUp <| double_printer n.NetworkDown)
//          Field.create "Jobs" Center (fun p -> sprintf "%d / %d" p.ActiveJobs p.MaxJobCount)
//          Field.create "Hostname" Left (fun p -> p.Hostname)
//          Field.create "Process Id" Right (fun p -> p.ProcessId)
//          Field.create "Heartbeat" Left (fun p -> p.HeartbeatTime)
//          Field.create "Initialization Time" Left (fun p -> p.InitializationTime) 
//        ]
//    
//    static member Report(workers : WorkerRef seq, title, borders) = 
//        let ws = workers
//                 |> Seq.sortBy (fun w -> w.InitializationTime)
//                 |> Seq.toList
//        Record.PrettyPrint(template, ws, title, borders)

//type internal LogReporter() = 
//    static let template : Field<LogRecord> list = 
//        [ Field.create "Source" Left (fun p -> p.PartitionKey)
//          Field.create "Timestamp" Right (fun p -> let pt = p.Time in pt.ToString("ddMMyyyy HH:mm:ss.fff zzz"))
//          Field.create "Message" Left (fun p -> p.Message) ]
//    
//    static member Report(logs : LogRecord seq, title, borders) = 
//        let ls = logs 
//                 |> Seq.sortBy (fun l -> l.Time, l.PartitionKey)
//                 |> Seq.toList
//        Record.PrettyPrint(template, ls, title, borders)
