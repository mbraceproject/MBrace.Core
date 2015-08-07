namespace MBrace.Thespian.Runtime

open System
open System.Collections.Generic

open Nessos.Thespian

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Library

open MBrace.Runtime
open MBrace.Runtime.Store

[<AutoOpen>]
module private ActorResultAggregator =

    type ResultAggregatorMsg =
        | SetResult of index:int * pvalue:ResultMessage<obj> * overwrite:bool * completed:IReplyChannel<bool>
        | GetCompleted of IReplyChannel<int>
        | IsCompleted of IReplyChannel<bool>
        | ToArray of IReplyChannel<ResultMessage<obj> []>

    /// <summary>
    ///     Creates a result aggregator actor in the local process.
    /// </summary>
    /// <param name="capacity"></param>
    let create (capacity : int) : ActorRef<ResultAggregatorMsg> =
        let behaviour (results : Map<int, ResultMessage<obj>>) msg = async {
            match msg with
            | SetResult(i, value, _, rc) when i < 0 || i >= capacity ->
                let e = new IndexOutOfRangeException()
                do! rc.ReplyWithException e
                return results

            | SetResult(i, value, false, rc) when results.ContainsKey i ->
                let e = new InvalidOperationException(sprintf "result at position '%d' has already been set." i)
                do! rc.ReplyWithException e
                return results

            | SetResult(i, value, _, rc) ->
                let results = results.Add(i, value)
                let isCompleted = results.Count = capacity
                do! rc.Reply isCompleted
                return results

            | GetCompleted rc ->
                do! rc.Reply results.Count
                return results

            | IsCompleted rc ->
                do! rc.Reply ((results.Count = capacity))
                return results

            | ToArray rc when results.Count = capacity ->
                let array = results |> Map.toSeq |> Seq.sortBy fst |> Seq.map snd |> Seq.toArray
                do! rc.Reply array
                return results

            | ToArray rc ->
                let e = new InvalidOperationException("Result aggregator incomplete.")
                do! rc.ReplyWithException e
                return results
        }

        Actor.Stateful Map.empty behaviour
        |> Actor.Publish
        |> Actor.ref

    /// A distributed resource that aggregates an array of results.
    [<AutoSerializable(true)>]
    type ActorResultAggregator<'T> internal (stateF : LocalStateFactory, capacity : int, source : ActorRef<ResultAggregatorMsg>) =

        interface ICloudResultAggregator<'T> with
            member __.Capacity = capacity
            member __.CurrentSize = source <!- GetCompleted
            member __.IsCompleted = source <!- IsCompleted
            member __.SetResult(index : int, value : 'T, overwrite : bool) = async {
                let state = stateF.Value
                let id = sprintf "parResult-%s" <| mkUUID()
                let! rm = state.CreateResult<obj>(value, allowNewSifts = false, fileName = id)
                return! source <!- fun ch -> SetResult(index, rm, overwrite, ch)
            }

            member __.ToArray () = async {
                let state = stateF.Value
                let! pickles = source <!- ToArray
                let getResult (rm : ResultMessage<obj>) = async {
                    let! result = state.ReadResult rm
                    return result :?> 'T
                }

                return! pickles |> Seq.map getResult |> Async.Parallel
            }

            member __.Dispose () = async.Zero()

/// Defines a distributed result aggregator factory
type ActorResultAggregatorFactory(factory : ResourceFactory, stateF : LocalStateFactory) =
    interface ICloudResultAggregatorFactory with
        member x.CreateResultAggregator(capacity: int): Async<ICloudResultAggregator<'T>> = async {
            let! ref = factory.RequestResource(fun () -> ActorResultAggregator.create capacity)
            return new ActorResultAggregator<'T>(stateF, capacity, ref) :> ICloudResultAggregator<'T>
        }