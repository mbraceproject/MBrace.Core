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

    let tryFindGen (predicate : ExecutionContext -> 'T -> bool) (flow : CloudFlow<'T>) : Cloud<'T option> =
        let collectorf (cloudCts : ICloudCancellationTokenSource) =
            local {
                let! ctx = Cloud.GetExecutionContext()
                let resultRef = ref Unchecked.defaultof<'T option>
                let cts = CancellationTokenSource.CreateLinkedTokenSource(cloudCts.Token.LocalToken)
                return
                    { new Collector<'T, 'T option> with
                        member self.DegreeOfParallelism = flow.DegreeOfParallelism
                        member self.Iterator() =
                            {   Func = (fun value -> if predicate ctx value then resultRef := Some value; cloudCts.Cancel() else ());
                                Cts = cts }
                        member self.Result =
                            !resultRef }
            }

        cloud {
            use! cts = Cloud.CreateLinkedCancellationTokenSource()
            return! flow.WithEvaluators (collectorf cts) (fun v -> local { return v }) (fun result -> local { return Array.tryPick id result })
        }

    let tryPickGen (chooser : ExecutionContext -> 'T -> 'R option) (flow : CloudFlow<'T>) : Cloud<'R option> =
        let collectorf (cloudCts : ICloudCancellationTokenSource) =
            local {
                let! ctx = Cloud.GetExecutionContext()
                let resultRef = ref Unchecked.defaultof<'R option>
                let cts = CancellationTokenSource.CreateLinkedTokenSource(cloudCts.Token.LocalToken)
                return
                    { new Collector<'T, 'R option> with
                        member self.DegreeOfParallelism = flow.DegreeOfParallelism
                        member self.Iterator() =
                            {   Func = (fun value -> match chooser ctx value with Some value' -> resultRef := Some value'; cloudCts.Cancel() | None -> ());
                                Cts = cts }
                        member self.Result =
                            !resultRef }
            }

        cloud {
            use! cts = Cloud.CreateLinkedCancellationTokenSource()
            return! flow.WithEvaluators (collectorf cts) (fun v -> local { return v }) (fun result -> local { return Array.tryPick id result })
        }