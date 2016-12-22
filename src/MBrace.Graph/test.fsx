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

let res = 
    CloudGraph.AggregateMessage<string, int, int> g (fun c -> cloud { return c.SendToSrc c.Attr }) 
        (fun acc m -> acc + m)
let a = res |> cluster.Run

a.TryFind "1"

let vertices : Node<int * int> [] = 
    [| { Id = 1L
         Attr = (7, -1) }
       { Id = 2L
         Attr = (3, -1) }
       { Id = 3L
         Attr = (2, -1) }
       { Id = 4L
         Attr = (6, -1) } |]

let relationships : Edge<bool> [] = 
    [| { SrcId = 1L
         DstId = 2L
         Attr = true }
       { SrcId = 1L
         DstId = 4L
         Attr = true }
       { SrcId = 2L
         DstId = 4L
         Attr = true }
       { SrcId = 3L
         DstId = 1L
         Attr = true }
       { SrcId = 3L
         DstId = 4L
         Attr = true } |]

let graph = 
    { Vertices = vertices |> CloudFlow.OfArray
      Edges = relationships |> CloudFlow.OfArray }

let initialMsg = 9999

let vprog (vertexId : VertexId, value : int * int, message : int) : int * int =
    if (message = initialMsg) then value
    else (min message (fst value), fst value)

let sendMsg (ctx : EdgeContext<int * int, bool, int>) : Cloud<unit> = 
    cloud { 
        let! sourceVertex = ctx.SrcAttr 
        if (fst sourceVertex <> snd sourceVertex) then 
            ctx.SendToDst (fst sourceVertex)
        return ()
    }

let mergeMsg (msg1 : int) (msg2 : int) : int = min msg1 msg2
let minGraph = 
    CloudGraph.Pregel graph initialMsg System.Int32.MaxValue EdgeDirection.Out vprog sendMsg mergeMsg |> cluster.Run

minGraph.Vertices
|> CloudFlow.toArray
|> cluster.Run
