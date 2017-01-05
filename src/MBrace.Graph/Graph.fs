﻿namespace MBrace.Graph

open MBrace.Core
open MBrace.Flow

type VertexId = int64

type Node<'a> = 
    { Id : VertexId
      Attr : 'a }

type Edge<'a> = 
    { SrcId : VertexId
      DstId : VertexId
      Attr : 'a }

type EdgeDirection = 
    | In
    | Out
    | Either
    | Both

type EdgeTriplet<'a, 'b> = 
    { SrcId : VertexId
      SrcAttr : 'a
      DstId : VertexId
      DstAttr : 'a
      Attr : 'b }

type EdgeContext<'a, 'b, 'c> = 
    { SrcId : VertexId
      SrcAttr : 'a
      DstId : VertexId
      DstAttr : 'a
      Attr : 'b
      SendToSrc : 'c -> unit
      SendToDst : 'c -> unit }

type Graph<'a, 'b> = 
    { Vertices : CloudFlow<Node<'a>>
      Edges : CloudFlow<Edge<'b>> }

module CloudGraph = 
    let inline AggregateMessages<'a, 'b, 'm> (sendMsg : EdgeContext<'a, 'b, 'm> -> unit) (accMsg : 'm -> 'm -> 'm) 
               (graph : Graph<'a, 'b>) : CloudFlow<VertexId * 'm> = 
        let joinSource = (graph.Edges, graph.Vertices) ||> CloudFlow.join (fun a -> a.Id) (fun a -> a.SrcId)
        let joinTarget = (joinSource, graph.Vertices) ||> CloudFlow.join (fun a -> a.Id) (fun (_, _, a) -> a.DstId)
        
        let messages = 
            joinTarget |> CloudFlow.collect (fun (_, dst, (_, src, edge)) -> 
                              let messages = new System.Collections.Generic.List<VertexId * 'm>()
                              
                              let ctx = 
                                  { SrcId = edge.SrcId
                                    SrcAttr = src.Attr
                                    DstId = edge.DstId
                                    DstAttr = dst.Attr
                                    Attr = edge.Attr
                                    SendToSrc = fun m -> messages.Add(edge.SrcId, m)
                                    SendToDst = fun m -> messages.Add(edge.DstId, m) }
                              sendMsg ctx
                              messages)
                        |> CloudFlow.groupBy (fun (id, _) -> id)
                        |> CloudFlow.map (fun (id, ms) -> id, ms |> Seq.map snd |> Seq.reduce accMsg)
        messages
    
    let inline OuterJoinVertices<'a, 'b, 'c, 'm> (messages : CloudFlow<VertexId * 'm>) 
               (vprog : VertexId * 'a * 'm option -> 'c) (graph : Graph<'a, 'b>) : Cloud<Graph<'c, 'b>> =
        cloud {
        let! vs = 
            (graph.Vertices, messages)
            ||> CloudFlow.rightOuterJoin (fun (id, _) -> id) (fun a -> a.Id)
            |> CloudFlow.map (fun (_, m, v) -> 
                   match m with
                   | Some(_, m) -> 
                       { Id = v.Id
                         Attr = vprog (v.Id, v.Attr, Some(m)) }
                   | _ -> 
                       { Id = v.Id
                         Attr = vprog (v.Id, v.Attr, None) })
            |> CloudFlow.persist StorageLevel.Memory
        return { Vertices = vs
                 Edges = graph.Edges } }
    
    let inline Pregel<'a, 'b, 'm> (initialMsg : 'm) (maxIterations : int) (activeDirection : EdgeDirection) 
               (vprog : VertexId * 'a * 'm -> 'a) (sendMsg : EdgeContext<'a, 'b, 'm> -> unit) 
               (mergeMsg : 'm -> 'm -> 'm) (graph : Graph<'a, 'b>) : LocalCloud<Graph<'a, 'b>> = 
        local {
            let! vs = graph.Vertices 
                      |> CloudFlow.map (fun v -> 
                                             { Id = v.Id
                                               Attr = (false, vprog (v.Id, v.Attr, initialMsg)) })
                      |> CloudFlow.persist StorageLevel.Memory
                      |> Cloud.AsLocal
            let mutable newGraph = 
                { Vertices = vs
                  Edges = graph.Edges }
            
            let mutable md =
                newGraph |> AggregateMessages (fun ctx -> 
                                sendMsg { SrcId = ctx.SrcId
                                          SrcAttr = snd ctx.SrcAttr
                                          DstId = ctx.DstId
                                          DstAttr = snd ctx.DstAttr
                                          Attr = ctx.Attr
                                          SendToSrc = ctx.SendToSrc
                                          SendToDst = ctx.SendToDst }) mergeMsg
            
            let mutable i = 0
            let! mcs = md
                       |> CloudFlow.countBy (fun _ -> 1)
                       |> CloudFlow.toArray
                       |> Cloud.AsLocal
            let mutable mc = if mcs.Length = 1 then snd mcs.[0] else 0L
            
            let sendMsgForActive ctx = 
                match activeDirection, fst ctx.SrcAttr, fst ctx.DstAttr with
                | EdgeDirection.Both, true, true 
                | EdgeDirection.Either, true, _ 
                | EdgeDirection.Either, _, true 
                | EdgeDirection.In, _, true 
                | EdgeDirection.Out, true, _ -> 
                    sendMsg { SrcId = ctx.SrcId
                              SrcAttr = snd ctx.SrcAttr
                              DstId = ctx.DstId
                              DstAttr = snd ctx.DstAttr
                              Attr = ctx.Attr
                              SendToSrc = ctx.SendToSrc
                              SendToDst = ctx.SendToDst }
                | _ -> ()
            while mc > 0L && i < maxIterations do
                let! g = newGraph 
                         |> OuterJoinVertices md (fun (id, (_, attr), m) -> m.IsSome, if m.IsSome then vprog (id, attr, m.Value) else attr)
                         |> Cloud.AsLocal
                newGraph <- g
                i <- i + 1
                md <- newGraph |> AggregateMessages sendMsgForActive mergeMsg
                let! mcs = md
                           |> CloudFlow.countBy (fun _ -> 1)
                           |> CloudFlow.toArray
                           |> Cloud.AsLocal
                mc <- if mcs.Length = 1 then snd mcs.[0] else 0L
            let! vs = newGraph.Vertices 
                      |> CloudFlow.map (fun n -> 
                                                  { Id = n.Id
                                                    Attr = snd n.Attr })
                      |> CloudFlow.persist StorageLevel.Memory
                      |> Cloud.AsLocal
            return { Vertices = vs
                     Edges = newGraph.Edges }
        }
    
    let inline Degrees (edgeDirection : EdgeDirection) (graph : Graph<'a, 'b>) = 
        match edgeDirection with
        | EdgeDirection.In -> graph |> AggregateMessages (fun ctx -> ctx.SendToDst 1) (fun acc m -> acc + m)
        | EdgeDirection.Out -> graph |> AggregateMessages (fun ctx -> ctx.SendToSrc 1) (fun acc m -> acc + m)
        | _ -> 
            graph |> AggregateMessages (fun ctx -> 
                         ctx.SendToSrc(1)
                         ctx.SendToDst(1)) (fun acc m -> acc + m)
    
    let inline MapVertices<'a, 'b, 'c> (mapVertices : Node<'a> -> 'c) (graph : Graph<'a, 'b>) : Cloud<Graph<'c, 'b>> = 
        cloud { 
            let! vertices = graph.Vertices
                            |> CloudFlow.map (fun n -> 
                                   { Id = n.Id
                                     Attr = mapVertices n })
                            |> CloudFlow.persist StorageLevel.Memory
            return { Vertices = vertices
                     Edges = graph.Edges }
        }
    
    let inline MapTriplets<'a, 'b, 'c> (mapTriplets : EdgeTriplet<'a, 'b> -> 'c) (graph : Graph<'a, 'b>) : Cloud<Graph<'a, 'c>> = 
        cloud { 
            let! vDict = CloudDictionary.New<'a>()
            do! graph.Vertices |> CloudFlow.iter (fun v -> vDict.TryAdd(v.Id.ToString(), v.Attr) |> ignore)
            let! edges = graph.Edges
                         |> CloudFlow.map (fun e -> 
                                let srcAttr = vDict.TryFind(e.SrcId.ToString())
                                let dstAttr = vDict.TryFind(e.DstId.ToString())
                                
                                let attr = 
                                    mapTriplets { SrcId = e.SrcId
                                                  SrcAttr = srcAttr.Value
                                                  DstId = e.DstId
                                                  DstAttr = dstAttr.Value
                                                  Attr = e.Attr }
                                { Edge.SrcId = e.SrcId
                                  DstId = e.DstId
                                  Attr = attr })
                         |> CloudFlow.persist StorageLevel.Memory
            return { Vertices = graph.Vertices
                     Edges = edges }
        }
    
    let inline PageRank<'a, 'b> (tol : double) (resetProb : double) (graph : Graph<'a, 'b>) = 
        cloud { 
            let outDegrees = graph |> Degrees EdgeDirection.Out
            
            let! pagerankGraph = 
                graph |> OuterJoinVertices outDegrees (fun (_, _, deg) -> 
                             match deg with
                             | Some x -> x
                             | _ -> 0)
            let! pagerankGraph = pagerankGraph |> MapTriplets(fun e -> 1.0 / float e.SrcAttr)
            let! pagerankGraph = pagerankGraph |> MapVertices(fun _ -> (0.0, 0.0))
            let vertexProgram (id : VertexId) (attr : double * double) (msgSum : double) : double * double = 
                let (oldPR, lastDelta) = attr
                let newPR = oldPR + (1.0 - resetProb) * msgSum
                (newPR, newPR - oldPR)
            
            let sendMessage (ctx : EdgeContext<double * double, double, double>) = 
                if snd ctx.SrcAttr > tol then ctx.SendToDst(snd ctx.SrcAttr * ctx.Attr)
            
            let messageCombiner (a : double) (b : double) : double = a + b
            let initialMessage = resetProb / (1.0 - resetProb)
            let vp (id : VertexId, attr : double * double, msgSum : double) = vertexProgram id attr msgSum
            let! pregelGraph = pagerankGraph 
                               |> Pregel initialMessage System.Int32.MaxValue EdgeDirection.Out vp sendMessage 
                                      messageCombiner
            let! res = pregelGraph |> MapVertices(fun { Id = vid; Attr = attr } -> fst attr)
            return res
        }
