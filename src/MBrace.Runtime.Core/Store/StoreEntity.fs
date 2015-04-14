namespace MBrace.Runtime.Store

open System.Collections.Generic
open System.Runtime.Serialization

open Nessos.FsPickler

open MBrace.Store
open MBrace.Store.Internals
open MBrace.Runtime.Utils

type StorageEntity =

    /// <summary>
    ///     Gathers all store entities that occur in object graph.
    ///     Can be used by runtime implementers to keep track of caching.
    /// </summary>
    /// <param name="graph">Serializable object graph to be traversed.</param>
    static member GatherStoreEntitiesInObjectGraph<'T when 'T : not struct>(graph : 'T) =
        let gathered = new Dictionary<string, ICloudStorageEntity> ()
        let visitor = 
            { new IObjectVisitor with
                member __.Visit(element : 'a) =
                    match box element with
                    | :? ICloudStorageEntity as e when not <| gathered.ContainsKey e.Id ->
                        gathered.Add(e.Id, e)
                    | _ -> ()
            }

        FsPickler.VisitObject(visitor, graph)
        gathered |> Seq.map (function KeyValue(_,e) -> e)