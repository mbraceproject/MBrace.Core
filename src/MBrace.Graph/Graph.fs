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
      SrcAttr : Cloud<'a>
      DstId : VertexId
      DstAttr : Cloud<'a>
      Attr : 'b }

type EdgeContext<'a, 'b, 'c> = 
    { SrcId : VertexId
      SrcAttr : Cloud<'a>
      DstId : VertexId
      DstAttr : Cloud<'a>
      Attr : 'b
      SendToSrc : 'c -> unit
      SendToDst : 'c -> unit }

type Graph<'a, 'b> = 
    { Vertices : CloudFlow<Node<'a>>
      Edges : CloudFlow<Edge<'b>> }

module CloudGraph = 
    let inline GetAttr id graph = 
        graph.Vertices |> CloudFlow.pick (fun a -> 
                              if a.Id = id then Some(a.Attr)
                              else None)
    
    let inline AggregateMessages<'a, 'b, 'm> (sendMsg : EdgeContext<'a, 'b, 'm> -> Cloud<unit>) 
               (accMsg : 'm -> 'm -> 'm) (graph : Graph<'a, 'b>) : Cloud<CloudDictionary<'m>> = 
        let sentTo (dict : CloudDictionary<'m>) (id : VertexId) accMsg m = 
            dict.AddOrUpdate(id.ToString(), 
                             (fun acc -> 
                             match acc with
                             | Some acc -> accMsg acc m
                             | _ -> m))
            |> ignore
        cloud { 
            use! dict = CloudDictionary.New<'m>()
            let! a = graph.Edges
                     |> CloudFlow.map (fun edge -> 
                            let ctx = 
                                { SrcId = edge.SrcId
                                  SrcAttr = graph |> GetAttr edge.SrcId
                                  DstId = edge.DstId
                                  DstAttr = graph |> GetAttr edge.DstId
                                  Attr = edge.Attr
                                  SendToSrc = sentTo dict edge.SrcId accMsg
                                  SendToDst = sentTo dict edge.DstId accMsg }
                            sendMsg ctx)
                     |> CloudFlow.toArray
            let! b = a |> Cloud.Parallel
            b |> ignore
            return dict
        }
    
    let inline JoinVertices<'a, 'b, 'm> (dict : CloudDictionary<'m>) (vprog : VertexId * 'a * 'm -> 'a) 
               (graph : Graph<'a, 'b>) : Cloud<Graph<'a, 'b>> = 
        cloud { 
            let! vs = graph.Vertices
                      |> CloudFlow.map (fun v -> 
                             match dict.TryFind(v.Id.ToString()) with
                             | Some m -> { v with Attr = vprog (v.Id, v.Attr, m) }
                             | _ -> v)
                      |> CloudFlow.persist StorageLevel.Memory
            return { graph with Vertices = vs }
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
               (vprog : VertexId * 'a * 'm -> 'a) (sendMsg : EdgeContext<'a, 'b, 'm> -> Cloud<unit>) 
               (mergeMsg : 'm -> 'm -> 'm) (graph : Graph<'a, 'b>) : LocalCloud<Graph<'a, 'b>> = 
        local { 
            let mutable g = 
                { graph with Vertices = 
                                 graph.Vertices |> CloudFlow.map (fun v -> 
                                                       { Id = v.Id
                                                         Attr = vprog (v.Id, v.Attr, initialMsg) }) }
            
            let mutable messages = 
                g
                |> AggregateMessages (fun ctx -> cloud { return! sendMsg ctx }) mergeMsg
                |> Cloud.AsLocal
            
            let! m = messages
            let mutable i = 0
            let mutable md = m
            
            let sendMsgForActive ctx = 
                cloud { 
                    let b = md.TryFindAsync(ctx.SrcId.ToString()) |> Async.RunSynchronously
                    let c = md.TryFindAsync(ctx.DstId.ToString()) |> Async.RunSynchronously
                    return! match activeDirection, b, c with
                            | EdgeDirection.Both, Some _, Some _ | EdgeDirection.Either, Some _, _ | EdgeDirection.Either, 
                                                                                                     _, Some _ | EdgeDirection.In, 
                                                                                                                 _, 
                                                                                                                 Some _ | EdgeDirection.Out, 
                                                                                                                          Some _, 
                                                                                                                          _ -> 
                                sendMsg ctx
                            | _ -> cloud { return () }
                }
            while md.GetCountAsync()
                  |> Async.RunSynchronously
                  > 0L
                  && i < maxIterations do
                let! newGraph = g
                                |> JoinVertices md vprog
                                |> Cloud.AsLocal
                g <- newGraph
                messages <- g
                            |> AggregateMessages sendMsgForActive mergeMsg
                            |> Cloud.AsLocal
                let! m = messages
                i <- i + 1
                md <- m
            return g
        }
    
    let inline Degrees (edgeDirection : EdgeDirection) (graph : Graph<'a, 'b>) = 
        match edgeDirection with
        | EdgeDirection.In -> 
            graph |> AggregateMessages (fun ctx -> cloud { return ctx.SendToDst 1 }) (fun acc m -> acc + m)
        | EdgeDirection.Out -> 
            graph |> AggregateMessages (fun ctx -> cloud { return ctx.SendToSrc 1 }) (fun acc m -> acc + m)
        | _ -> 
            graph |> AggregateMessages (fun ctx -> 
                         cloud { 
                             return ctx.SendToSrc(1)
                             ctx.SendToDst(1)
                         }) (fun acc m -> acc + m)
    
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
    
    let inline MapTriplets<'a, 'b, 'c> (mapTriplets : EdgeTriplet<'a, 'b> -> Cloud<'c>) (graph : Graph<'a, 'b>) : Cloud<Graph<'a, 'c>> = 
        cloud { 
            let! edges = graph.Edges
                         |> CloudFlow.map (fun e ->
                                cloud { 
                                let! attr = mapTriplets { SrcId = e.SrcId
                                                          SrcAttr = graph |> GetAttr e.SrcId
                                                          DstId = e.DstId
                                                          DstAttr = graph |> GetAttr e.DstId
                                                          Attr = e.Attr }
                                return { Edge.SrcId = e.SrcId
                                         DstId = e.DstId
                                         Attr = attr }})
                         |> CloudFlow.toArray
            let! edges = edges |> Cloud.Parallel
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
            let! pagerankGraph = pagerankGraph |> MapTriplets (fun e -> cloud { let! scrAttr = e.SrcAttr
                                                                                return 1.0 / float scrAttr })
            let! pagerankGraph = pagerankGraph |> MapVertices (fun _ -> (0.0, 0.0))

            let vertexProgram (id: VertexId) (attr: double * double) (msgSum: double): double * double =
                let (oldPR, lastDelta) = attr
                let newPR = oldPR + (1.0 - resetProb) * msgSum
                (newPR, newPR - oldPR)

            let sendMessage (ctx: EdgeContext<double * double, double, double>) =
                cloud {
                let! srcAttr = ctx.SrcAttr
                if snd srcAttr > tol then
                    ctx.SendToDst (snd srcAttr * ctx.Attr) }
            
            let messageCombiner (a: double) (b: double) : double = a + b

            let initialMessage = resetProb / (1.0 - resetProb)

            let vp (id: VertexId, attr: double * double, msgSum: double) =
                vertexProgram id attr msgSum

            let! prefelGraph = pagerankGraph |> Pregel initialMessage System.Int32.MaxValue EdgeDirection.Out vp sendMessage messageCombiner
            let! res = prefelGraph |> MapVertices(fun { Id = vid ; Attr = attr } -> fst attr)

            return res
        }
