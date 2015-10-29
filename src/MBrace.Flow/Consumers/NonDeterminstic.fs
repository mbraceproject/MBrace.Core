namespace MBrace.Flow.Internals

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

module NonDeterministic =

    let tryFind (predicate : 'T -> bool) (flow : CloudFlow<'T>) : Cloud<'T option> = cloud {
        let collectorf (cloudCts : ICloudCancellationTokenSource) = local {
                let resultRef = ref Unchecked.defaultof<'T option>
                let cts = CancellationTokenSource.CreateLinkedTokenSource(cloudCts.Token.LocalToken)
                return
                    { new Collector<'T, 'T option> with
                        member self.DegreeOfParallelism = flow.DegreeOfParallelism
                        member self.Iterator() =
                            {   Func = (fun value -> if predicate value then resultRef := Some value; cloudCts.Cancel() else ());
                                Cts = cts }
                        member self.Result =
                            !resultRef }
            }

        use! cts = Cloud.CreateLinkedCancellationTokenSource()
        return! flow.WithEvaluators (collectorf cts) (fun v -> local { return v }) (fun result -> local { return Array.tryPick id result })
    }

    let tryPick (chooser : 'T -> 'R option) (flow : CloudFlow<'T>) : Cloud<'R option> = cloud {
        let collectorf (cloudCts : ICloudCancellationTokenSource) = local {
            let resultRef = ref Unchecked.defaultof<'R option>
            let cts = CancellationTokenSource.CreateLinkedTokenSource(cloudCts.Token.LocalToken)
            return
                { new Collector<'T, 'R option> with
                    member self.DegreeOfParallelism = flow.DegreeOfParallelism
                    member self.Iterator() =
                        {   Func = (fun value -> match chooser value with Some value' -> resultRef := Some value'; cloudCts.Cancel() | None -> ());
                            Cts = cts }
                    member self.Result =
                        !resultRef }
        }

        use! cts = Cloud.CreateLinkedCancellationTokenSource()
        return! flow.WithEvaluators (collectorf cts) (fun v -> local { return v }) (fun result -> local { return Array.tryPick id result })
    }