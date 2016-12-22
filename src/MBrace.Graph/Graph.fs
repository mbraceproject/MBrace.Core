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
    let inline AggregateMessage<'a, 'b, 'm> (graph : Graph<'a, 'b>) (sendMsg : EdgeContext<'a, 'b, 'm> -> Cloud<unit>) 
               (accMsg : 'm -> 'm -> 'm) = 
        let getAttr id = 
            graph.Vertices |> CloudFlow.pick (fun a -> 
                                  if a.Id = id then Some(a.Attr)
                                  else None)
        
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
                                  SrcAttr = getAttr edge.SrcId
                                  DstId = edge.DstId
                                  DstAttr = getAttr edge.DstId
                                  Attr = edge.Attr
                                  SendToSrc = sentTo dict edge.SrcId accMsg
                                  SendToDst = sentTo dict edge.DstId accMsg }
                            sendMsg ctx)
                     |> CloudFlow.toArray
            let! b = a |> Cloud.Parallel
            b |> ignore
            return dict
        }
    
    let inline Pregel<'a, 'b, 'm> (graph : Graph<'a, 'b>) (initialMsg : 'm) (maxIterations : int) 
               (activeDirection : EdgeDirection) (vprog : VertexId * 'a * 'm -> 'a) 
               (sendMsg : EdgeContext<'a, 'b, 'm> -> Cloud<unit>) (mergeMsg : 'm -> 'm -> 'm) = 
        local {
            let mutable g = 
                { graph with Vertices = 
                                 graph.Vertices |> CloudFlow.map (fun v -> 
                                                       { Id = v.Id
                                                         Attr = vprog (v.Id, v.Attr, initialMsg) }) }
            let mutable messages = 
                AggregateMessage g (fun ctx -> 
                    cloud { 
                        let! a = sendMsg ctx
                        return a
                    }) mergeMsg |> Cloud.AsLocal
            
            let! m = messages
            let mutable i = 0
            let mutable md = m
            while md.GetCountAsync() |> Async.RunSynchronously > 0L && i < maxIterations do
                let! vs = g.Vertices |> CloudFlow.map (fun v -> 
                                                    match md.TryFind(v.Id.ToString()) with
                                                    | Some m -> { v with Attr = vprog (v.Id, v.Attr, m) }
                                                    | _ -> v)
                                     |> CloudFlow.toArray
                                     |> Cloud.AsLocal
                g <- { g with Vertices = vs |> CloudFlow.OfArray }
                messages <- AggregateMessage g (fun ctx -> 
                                cloud { 
                                    let b = md.TryFindAsync(ctx.SrcId.ToString()) |> Async.RunSynchronously
                                    let c = md.TryFindAsync(ctx.DstId.ToString()) |> Async.RunSynchronously
                                    let! a = match activeDirection, b, c with
                                             | EdgeDirection.Both, Some _, Some _ 
                                             | EdgeDirection.Either, Some _, _ 
                                             | EdgeDirection.Either,  _,  Some _ 
                                             | EdgeDirection.In, _, Some _ 
                                             | EdgeDirection.Out, Some _, _ -> 
                                                 sendMsg ctx
                                             | _ -> cloud { return () }
                                    return a
                                }) mergeMsg |> Cloud.AsLocal
                let! m = messages
                i <- i + 1
                md <- m
            return g
        }
