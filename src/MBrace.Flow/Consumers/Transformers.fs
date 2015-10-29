namespace MBrace.Flow.Internals.Consumers

open System.IO
open System.Runtime.Serialization
open System.Collections.Generic
open System.Threading

open Nessos.Streams
open Nessos.Streams.Internals

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Flow

#nowarn "444"

module Transformers =

    /// <summary>Transforms each element of the input CloudFlow.</summary>
    /// <param name="f">A function to transform items from the input CloudFlow.</param>
    /// <param name="flow">The input CloudFlow.</param>
    /// <returns>The result CloudFlow.</returns>
    let map (f : 'T -> 'R) (flow : CloudFlow<'T>) : CloudFlow<'R> =
        { new CloudFlow<'R> with
            member self.DegreeOfParallelism = flow.DegreeOfParallelism
            member self.WithEvaluators<'S, 'Result> collectorf (projection : 'S -> LocalCloud<'Result>) combiner =
                let collectorf' = local {
                    let! collector = collectorf
                    return
                      { new Collector<'T, 'S> with
                        member self.DegreeOfParallelism = collector.DegreeOfParallelism
                        member self.Iterator() =
                            let { Func = iter } as iterator = collector.Iterator()
                            {   Func = iter << f
                                Cts = iterator.Cts }
                        member self.Result = collector.Result  }
                }
                flow.WithEvaluators collectorf' projection combiner }

    /// <summary>Perform a monadic side-effect for each flow element.</summary>
    /// <param name="f">Monadic side-effect function.</param>
    /// <param name="flow">The input CloudFlow.</param>
    /// <returns>The result CloudFlow.</returns>
    let peek (f : 'T -> LocalCloud<unit>) (flow : CloudFlow<'T>) : CloudFlow<'T> =
        { new CloudFlow<'T> with
            member self.DegreeOfParallelism = flow.DegreeOfParallelism
            member self.WithEvaluators<'S, 'Result> collectorf (projection : 'S -> LocalCloud<'Result>) combiner =
                let collectorf' = local {
                    let! collector = collectorf
                    let! ctx = Cloud.GetExecutionContext()
                    return
                      { new Collector<'T, 'S> with
                        member self.DegreeOfParallelism = collector.DegreeOfParallelism
                        member self.Iterator() =
                            let { Func = iter } as iterator = collector.Iterator()
                            {   Func = fun value -> Cloud.RunSynchronously(f value, ctx.Resources, ctx.CancellationToken) ; iter value
                                Cts = iterator.Cts }
                        member self.Result = collector.Result  }
                }

                flow.WithEvaluators collectorf' projection combiner }

    /// <summary>
    ///     Filter flow transformer.
    /// </summary>
    /// <param name="predicate"></param>
    /// <param name="flow"></param>
    let filter (predicate : 'T -> bool) (flow : CloudFlow<'T>) : CloudFlow<'T> =
        { new CloudFlow<'T> with
            member self.DegreeOfParallelism = flow.DegreeOfParallelism
            member self.WithEvaluators<'S, 'R> (collectorf : LocalCloud<Collector<'T, 'S>>) (projection : 'S -> LocalCloud<'R>) combiner =
                let collectorf' = local {
                    let! collector = collectorf
                    return { new Collector<'T, 'S> with
                        member self.DegreeOfParallelism = collector.DegreeOfParallelism
                        member self.Iterator() =
                            let { Func = iter } as iterator = collector.Iterator()
                            {   Func = (fun value -> if predicate value then iter value else ());
                                Cts = iterator.Cts }
                        member self.Result = collector.Result }
                }
                flow.WithEvaluators collectorf' projection combiner }

    /// <summary>
    ///     Choose CloudFlow transformer.
    /// </summary>
    /// <param name="chooser"></param>
    /// <param name="flow"></param>
    let choose (chooser : 'T -> 'R option) (flow : CloudFlow<'T>) : CloudFlow<'R> =
        { new CloudFlow<'R> with
            member self.DegreeOfParallelism = flow.DegreeOfParallelism
            member self.WithEvaluators<'S, 'Result> collectorf (projection : 'S -> LocalCloud<'Result>) combiner =
                let collectorf' = local {
                    let! collector = collectorf
                    return { new Collector<'T, 'S> with
                        member self.DegreeOfParallelism = collector.DegreeOfParallelism
                        member self.Iterator() =
                            let { Func = iter } as iterator = collector.Iterator()
                            {
                                Func = (fun value ->
                                          match chooser value with
                                          | Some value' -> iter value'
                                          | None -> ())
                                Cts = iterator.Cts }
                        member self.Result = collector.Result }
                }
                flow.WithEvaluators collectorf' projection combiner }

    /// <summary>
    ///     Collect CloudFlow transformer.
    /// </summary>
    /// <param name="f">Collector function.</param>
    /// <param name="flow">Input cloud flow.</param>
    let collect (f : 'T -> #seq<'R>) (flow : CloudFlow<'T>) : CloudFlow<'R> =
        { new CloudFlow<'R> with
            member self.DegreeOfParallelism = flow.DegreeOfParallelism
            member self.WithEvaluators<'S, 'Result> collectorf (projection : 'S -> LocalCloud<'Result>) combiner =
                let collectorf' = local {
                    let! collector = collectorf
                    return
                      { new Collector<'T, 'S> with
                        member self.DegreeOfParallelism = collector.DegreeOfParallelism
                        member self.Iterator() =
                            let { Func = iter } as iterator = collector.Iterator()
                            {   Func = fun value -> Seq.iter iter (f value)
                                Cts = iterator.Cts }
                        member self.Result = collector.Result  }
                }
                flow.WithEvaluators collectorf' projection combiner }