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

module Take =

    /// <summary> Returns the elements of a CloudFlow up to a specified count. </summary>
    /// <param name="n">The maximum number of items to take.</param>
    /// <param name="flow">The input CloudFlow.</param>
    /// <returns>The resulting CloudFlow.</returns>
    let take (n : int) (flow: CloudFlow<'T>) : CloudFlow<'T> =
        let collectorF (cloudCts : ICloudCancellationTokenSource) =
            local {
                let results = new List<List<'T>>()
                let cts = CancellationTokenSource.CreateLinkedTokenSource(cloudCts.Token.LocalToken)
                return
                    { new Collector<'T, 'T []> with
                      member __.DegreeOfParallelism = flow.DegreeOfParallelism
                      member __.Iterator() =
                          let list = new List<'T>()
                          results.Add(list)
                          { Index = ref -1
                            Func = (fun value -> if list.Count < n then list.Add(value) else cloudCts.Cancel())
                            Cts = cts }
                      member __.Result =
                          (results |> Seq.concat).Take(n) |> Seq.toArray
                     }
            }
        let gather =
            cloud {
                use! cts = Cloud.CreateCancellationTokenSource()
                let! flow = flow.WithEvaluators (collectorF cts) (fun value -> PersistedCloudFlow.New(value, storageLevel = StorageLevel.Disk) ) 
                                                                 (fun results -> local { return PersistedCloudFlow.Concat results } )

                // Calculate number of partitions up to n
                let partitions = ResizeArray<_>()
                let totalCount = ref 0
                for (workerRef, cloudArray) in flow.GetPartitions() do
                    if !totalCount < n then
                        let count = cloudArray.GetCount() |> Async.RunSynchronously
                        if !totalCount + int count <= n then
                            partitions.Add(workerRef, cloudArray)
                        else 
                            let! cloudArray = CloudValue.NewArray(cloudArray.Take(n - !totalCount).ToArray(), storageLevel = StorageLevel.Disk)
                            partitions.Add(workerRef, cloudArray)
                        totalCount := !totalCount + int count
                return new PersistedCloudFlow<_>(partitions.ToArray())
            }
        { new CloudFlow<'T> with
              member __.DegreeOfParallelism = flow.DegreeOfParallelism
              member __.WithEvaluators<'S, 'R>(collectorF: Local<Collector<'T, 'S>>) (projection: 'S -> Local<'R>) combiner =
                  cloud {
                      let! flow = gather 
                      return! (flow :> CloudFlow<_>).WithEvaluators collectorF projection combiner
                  }
        }