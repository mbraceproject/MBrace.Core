namespace MBrace.Flow.Internals.Consumers

open System
open System.IO
open System.Linq
open System.Collections.Generic
open System.Collections.Concurrent
open System.Threading

open Nessos.Streams
open Nessos.Streams.Internals

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Flow

#nowarn "444"

module Sort =

    /// <summary>
    ///     Generic comparer CloudFlow combinator
    /// </summary>
    /// <param name="comparer">Optional custom comparer.</param>
    /// <param name="descending">Specify if comparison should be descending</param>
    /// <param name="projection">Projection function</param>
    /// <param name="takeCount">Top number of elements to keep in sorting</param>
    /// <param name="source">Source CloudFlow</param>
    let sortByGen (comparer : IComparer<'Key> option) (descending : bool) (projection : 'T -> 'Key) (takeCount : int) (source : CloudFlow<'T>) : CloudFlow<'T> =
        let sortByComp () = cloud {
            let collectorF = local {
                let results = new List<List<'T>>()
                let! ct = Cloud.CancellationToken
                let cts = CancellationTokenSource.CreateLinkedTokenSource ct.LocalToken
                return { 
                    new Collector<'T, 'Key[] * 'T []> with
                        member self.DegreeOfParallelism = source.DegreeOfParallelism
                        member self.Iterator() =
                            let list = new List<'T>()
                            results.Add(list)
                            {   Func = (fun value -> list.Add(value));
                                Cts = cts }
                        member self.Result =
                            let count = results |> Seq.sumBy (fun list -> list.Count)
                            let keys = Array.zeroCreate<'Key> count
                            let values = Array.zeroCreate<'T> count
                            let mutable counter = -1
                            for list in results do
                                for i = 0 to list.Count - 1 do
                                    let value = list.[i]
                                    counter <- counter + 1
                                    keys.[counter] <- projection value
                                    values.[counter] <- value

                            match comparer with
                            | None -> Sort.parallelSort Environment.ProcessorCount descending keys values
                            | Some cmp -> Sort.parallelSortWithComparer Environment.ProcessorCount cmp keys values

                            keys.Take(takeCount).ToArray(), values.Take(takeCount).ToArray()
                    }
            }

            let combine (results : ('Key [] * 'T []) []) = local { 
                let kss, tss = results |> Array.unzip
                return Array.concat kss, Array.concat tss
            }

            // perform distributed sorting
            let! keys, values = source.WithEvaluators collectorF local.Return combine

            // finally, merge the sorted results
            match comparer with
            | None -> Sort.parallelSort Environment.ProcessorCount descending keys values
            | Some cmp -> Sort.parallelSortWithComparer Environment.ProcessorCount cmp keys values

            let sortedValues = values.Take(takeCount).ToArray()
            return Array.ToCloudFlow(sortedValues, ?degreeOfParallelism = source.DegreeOfParallelism)
        }

        { new CloudFlow<'T> with
            member self.DegreeOfParallelism = source.DegreeOfParallelism
            member self.WithEvaluators<'S, 'R> (collectorf : LocalCloud<Collector<'T, 'S>>) (projection : 'S -> LocalCloud<'R>) combiner = cloud {
                let! sortedFlow = sortByComp ()
                return! sortedFlow.WithEvaluators collectorf projection combiner
            }
        }