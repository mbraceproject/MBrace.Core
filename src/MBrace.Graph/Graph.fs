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
    let inline AggregateMessage<'a, 'b, 'm> (g : Graph<'a, 'b>) (sendMsg : EdgeContext<'a, 'b, 'm> -> unit) 
               (accMsg : 'm -> 'm -> 'm) = 
        let getAttr id = 
            g.Vertices |> CloudFlow.pick (fun a -> 
                              if a.Id = id then Some(a.Attr)
                              else None)
        
        let sentTo (dict:CloudDictionary<'m>) (id:VertexId) accMsg m = 
            dict.AddOrUpdate(id.ToString(), 
                             (fun acc -> 
                             match acc with
                             | Some acc -> accMsg acc m
                             | _ -> m))
            |> ignore
        
        cloud { 
            use! dict = CloudDictionary.New<'m>()
            let! a = g.Edges
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
            a |> ignore
            return dict
        }
