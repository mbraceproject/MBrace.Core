namespace MBrace.Graph

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
    let inline internal aggregateMessages<'a, 'b, 'm> (sendMsg : EdgeContext<'a, 'b, 'm> -> unit) 
               (accMsg : 'm -> 'm -> 'm) (verticesDict : CloudDictionary<'a>) (edges : CloudFlow<Edge<'b>>) : Cloud<CloudDictionary<'m>> = 
        let sentTo (dict : CloudDictionary<'m>) (id : VertexId) accMsg m = 
            dict.AddOrUpdate(id.ToString(), 
                             (fun acc -> 
                             match acc with
                             | Some acc -> accMsg acc m
                             | _ -> m))
            |> ignore
        cloud { 
            let! dict = CloudDictionary.New<'m>()
            do! edges
                     |> CloudFlow.iter (fun edge -> 
                            let srcAttr = verticesDict.TryFind (edge.SrcId.ToString())
                            let dstAttr = verticesDict.TryFind (edge.DstId.ToString())
                            let ctx = 
                                { SrcId = edge.SrcId
                                  SrcAttr = srcAttr.Value
                                  DstId = edge.DstId
                                  DstAttr = dstAttr.Value
                                  Attr = edge.Attr
                                  SendToSrc = sentTo dict edge.SrcId accMsg
                                  SendToDst = sentTo dict edge.DstId accMsg }
                            sendMsg ctx)
            return dict
        }

    let inline AggregateMessages<'a, 'b, 'm> (sendMsg : EdgeContext<'a, 'b, 'm> -> unit) 
               (accMsg : 'm -> 'm -> 'm) (graph : Graph<'a, 'b>) : Cloud<CloudDictionary<'m>> = 
        cloud { 
            let! verticesDict = CloudDictionary.New<'a>()
            do! graph.Vertices |> CloudFlow.iter (fun v -> verticesDict.TryAdd(v.Id.ToString(), v.Attr) |> ignore)
            return! aggregateMessages sendMsg accMsg verticesDict graph.Edges
        }
    
    let inline internal joinVertices<'a, 'm> (dict : CloudDictionary<'m>) (vprog : VertexId * 'a * 'm -> 'a) 
               (vertices : CloudDictionary<'a>) : Cloud<CloudDictionary<'a>> = 
        cloud {
            let! verticesDict = CloudDictionary.New<'a>()
            do! vertices
                |> CloudFlow.OfCloudCollection
                |> CloudFlow.iter (fun v -> 
                       match dict.TryFind(v.Key) with
                       | Some m -> verticesDict.TryAdd(v.Key, vprog (int64 v.Key, v.Value, m)) |> ignore
                       | _ -> verticesDict.TryAdd(v.Key, v.Value) |> ignore)
            return verticesDict
        }
    
    let inline OuterJoinVertices<'a, 'b, 'c, 'm> (dict : CloudDictionary<'m>) (vprog : VertexId * 'a * 'm option -> 'c) 
               (graph : Graph<'a, 'b>) : Cloud<Graph<'c, 'b>> = 
        cloud { 
            let! vs = graph.Vertices
                      |> CloudFlow.map (fun v -> 
                             match dict.TryFind(v.Id.ToString()) with
                             | Some m -> 
                                 { Id = v.Id
                                   Attr = vprog (v.Id, v.Attr, Some(m)) }
                             | _ -> 
                                 { Id = v.Id
                                   Attr = vprog (v.Id, v.Attr, None) })
                      |> CloudFlow.persist StorageLevel.Memory
            return { Vertices = vs
                     Edges = graph.Edges }
        }
    
    let inline Pregel<'a, 'b, 'm> (initialMsg : 'm) (maxIterations : int) (activeDirection : EdgeDirection) 
               (vprog : VertexId * 'a * 'm -> 'a) (sendMsg : EdgeContext<'a, 'b, 'm> -> unit) 
               (mergeMsg : 'm -> 'm -> 'm) (graph : Graph<'a, 'b>) : LocalCloud<Graph<'a, 'b>> = 
        local {            
            let! verticesDict = CloudDictionary.New<'a>()
            do! graph.Vertices |> CloudFlow.iter (fun v -> verticesDict.TryAdd(v.Id.ToString(), vprog (v.Id, v.Attr, initialMsg)) |> ignore) |> Cloud.AsLocal
                
            
            let! m = graph.Edges
                     |> aggregateMessages (fun ctx -> sendMsg ctx) mergeMsg verticesDict
                     |> Cloud.AsLocal
            let mutable i = 0
            let mutable md = m
            let mutable dict = verticesDict
            
            let sendMsgForActive ctx =  
                    let b = md.TryFindAsync(ctx.SrcId.ToString()) |> Async.RunSynchronously
                    let c = md.TryFindAsync(ctx.DstId.ToString()) |> Async.RunSynchronously
                    match activeDirection, b, c with
                    | EdgeDirection.Both, Some _, Some _ 
                    | EdgeDirection.Either, Some _, _ 
                    | EdgeDirection.Either, _, Some _ 
                    | EdgeDirection.In, _, Some _ 
                    | EdgeDirection.Out, Some _, _ -> 
                        sendMsg ctx
                    | _ -> ()

            while md.GetCountAsync()
                  |> Async.RunSynchronously
                  > 0L
                  && i < maxIterations do
                let! newDict = dict
                               |> joinVertices md vprog
                               |> Cloud.AsLocal
                dict <- newDict

                let! m = graph.Edges
                         |> aggregateMessages sendMsgForActive mergeMsg dict
                         |> Cloud.AsLocal
                i <- i + 1
                md <- m
            return { graph with Vertices = dict |> CloudFlow.OfCloudCollection |> CloudFlow.map (fun v -> { Id = int64 v.Key ; Attr = v.Value })}
        }
    
    let inline Degrees (edgeDirection : EdgeDirection) (graph : Graph<'a, 'b>) = 
        match edgeDirection with
        | EdgeDirection.In -> 
            graph |> AggregateMessages (fun ctx -> ctx.SendToDst 1) (fun acc m -> acc + m)
        | EdgeDirection.Out -> 
            graph |> AggregateMessages (fun ctx -> ctx.SendToSrc 1) (fun acc m -> acc + m)
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
                                    let srcAttr = vDict.TryFind (e.SrcId.ToString())
                                    let dstAttr = vDict.TryFind (e.DstId.ToString())
                                    let attr = mapTriplets { SrcId = e.SrcId
                                                             SrcAttr = srcAttr.Value
                                                             DstId = e.DstId
                                                             DstAttr = dstAttr.Value
                                                             Attr = e.Attr }
                                    { Edge.SrcId = e.SrcId
                                      DstId = e.DstId
                                      Attr = attr })
                         |> CloudFlow.toArray
            return { Vertices = graph.Vertices
                     Edges = edges |> CloudFlow.OfArray }
        }
    
    let inline PageRank<'a, 'b> (tol : double) (resetProb : double) (graph : Graph<'a, 'b>) = 
        cloud { 
            let! outDegrees = graph |> Degrees EdgeDirection.Out
            let! pagerankGraph = graph |> OuterJoinVertices outDegrees (fun (vid, vdata, deg) -> 
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
            let! prefelGraph = pagerankGraph 
                               |> Pregel initialMessage System.Int32.MaxValue EdgeDirection.Out vp sendMessage 
                                      messageCombiner
            let! res = prefelGraph |> MapVertices(fun { Id = vid; Attr = attr } -> fst attr)
            return res
        }
