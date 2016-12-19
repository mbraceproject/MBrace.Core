#I "../../bin/"
#r "MBrace.Core.dll"
#r "MBrace.Runtime.dll"
#r "MBrace.Thespian.dll"
#r "MBrace.Flow.dll"
#r "MBrace.Graph.dll"
#r "Streams.dll"

open System
open MBrace.Core
open MBrace.Library
open MBrace.Thespian
open MBrace.Flow
open MBrace.Flow.Fluent
open MBrace.ThreadPool
open MBrace.Graph

ThespianWorker.LocalExecutable <- __SOURCE_DIRECTORY__ + "/../../bin/mbrace.thespian.worker.exe"

let cluster = 
    ThespianCluster.InitOnCurrentMachine(workerCount = 4, logLevel = LogLevel.Debug, logger = new ConsoleLogger())

let nodes : Node<string> [] = 
    [| { Id = 1L
         Attr = "A" }
       { Id = 2L
         Attr = "B" }
       { Id = 3L
         Attr = "C" }
       { Id = 4L
         Attr = "D" }
       { Id = 5L
         Attr = "E" } |]

let edges : Edge<int> [] = 
    [| { SrcId = 1L
         DstId = 2L
         Attr = 10 }
       { SrcId = 1L
         DstId = 3L
         Attr = 20 }
       { SrcId = 1L
         DstId = 4L
         Attr = 30 }
       { SrcId = 1L
         DstId = 5L
         Attr = 40 }
       { SrcId = 1L
         DstId = 1L
         Attr = 50 } |]

let g = 
    { Vertices = nodes |> CloudFlow.OfArray
      Edges = edges |> CloudFlow.OfArray }

let res = CloudGraph.AggregateMessage<string, int, int> g (fun c -> c.SendToSrc c.Attr) (fun acc m -> acc + m)
let a = res |> cluster.Run
a.TryFind "1"