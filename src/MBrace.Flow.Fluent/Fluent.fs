namespace MBrace.Flow.Fluent

open System.Collections.Generic
open MBrace
open MBrace.Core
open MBrace.Store
open MBrace.Flow

[<System.Runtime.CompilerServices.Extension>]
type CloudFlowExtensions() =
    static member inline map (this : CloudFlow<'T>, f : 'T -> 'R) : CloudFlow<'R> = CloudFlow.map f this
    static member inline mapLocal (this : CloudFlow<'T>, f : 'T -> Local<'R>) : CloudFlow<'R> = CloudFlow.mapLocal f this
    static member inline collect (this : CloudFlow<'T>, f : 'T -> seq<'R>) : CloudFlow<'R> = CloudFlow.collect f this
    static member inline collectLocal (this : CloudFlow<'T>, f : 'T -> Local<seq<'R>>) : CloudFlow<'R> = CloudFlow.collectLocal f this
    static member inline filter (this : CloudFlow<'T>, predicate : 'T -> bool) : CloudFlow<'T> = CloudFlow.filter predicate this
    static member inline filterLocal (this : CloudFlow<'T>, predicate : 'T -> Local<bool>) : CloudFlow<'T> = CloudFlow.filterLocal predicate this
    static member inline withDegreeOfParallelism (this : CloudFlow<'T>, degreeOfParallelism : int) : CloudFlow<'T> = CloudFlow.withDegreeOfParallelism degreeOfParallelism this
    static member inline fold (this : CloudFlow<'T>, folder : 'State -> 'T -> 'State, combiner : 'State -> 'State -> 'State, state : unit -> 'State) = CloudFlow.fold folder combiner state this
    static member inline foldByLocal (this : CloudFlow<'T>,
                                      projection : 'T -> Local<'Key>,
                                      folder : 'State -> 'T -> Local<'State>,
                                      combiner : 'State -> 'State -> Local<'State>,
                                      state : unit -> Local<'State>)  : CloudFlow<'Key * 'State> =
        CloudFlow.foldByLocal projection folder combiner state this

    static member inline foldBy (this : CloudFlow<'T>,
                                 projection : 'T -> 'Key,
                                 folder : 'State -> 'T -> 'State,
                                 combiner : 'State -> 'State -> 'State,
                                 state : unit -> 'State) : CloudFlow<'Key * 'State> =
        CloudFlow.foldBy projection folder combiner state this

    static member inline countBy (this : CloudFlow<'T>, projection : 'T -> 'Key) : CloudFlow<'Key * int64> = CloudFlow.countBy projection this
    static member inline countByLocal (this : CloudFlow<'T>, projection : 'T -> Local<'Key>) : CloudFlow<'Key * int64> = CloudFlow.countByLocal projection this
    static member inline iter (this : CloudFlow<'T>, action: 'T -> unit) : Cloud< unit > = CloudFlow.iter action this
    static member inline iterLocal (this : CloudFlow<'T>, action: 'T -> Local<unit>) : Cloud<unit> = CloudFlow.iterLocal action this

    static member inline sum (flow : CloudFlow< ^T >) : Cloud< ^T >
        when ^T : (static member ( + ) : ^T * ^T -> ^T)
        and  ^T : (static member Zero : ^T) =
            CloudFlow.sum flow


    static member inline sumBy (this : CloudFlow<'T>, f : 'T -> ^S) : Cloud< ^S >
        when ^S : (static member ( + ) : ^S * ^S -> ^S)
        and  ^S : (static member Zero : ^S) =
            CloudFlow.sumBy f this


    static member inline sumByLocal (this : CloudFlow<'T>, f : 'T -> Local< ^S >) : Cloud< ^S >
        when ^S : (static member ( + ) : ^S * ^S -> ^S)
        and  ^S : (static member Zero : ^S) =
            CloudFlow.sumByLocal f this


    static member inline length (this : CloudFlow<'T>) : Cloud<int64> = CloudFlow.length this

    static member inline toArray (this : CloudFlow<'T>) : Cloud<'T[]> = CloudFlow.toArray this
    static member inline toCloudVector (this : CloudFlow<'T>) : Cloud<CloudVector<'T>> = CloudFlow.toCloudVector this
    static member inline toCachedCloudVector (this : CloudFlow<'T>) : Cloud<CloudVector<'T>> = CloudFlow.toCachedCloudVector this

    static member inline sortBy (this : CloudFlow<'T>, projection : 'T -> 'Key, takeCount : int) : CloudFlow<'T> = CloudFlow.sortBy projection takeCount this
    static member inline sortByUsing (this : CloudFlow<'T>, projection : 'T -> 'Key, comparer : IComparer<'Key>, takeCount : int) : CloudFlow<'T> = CloudFlow.sortByUsing projection comparer takeCount this
    static member inline sortByDescending (this : CloudFlow<'T>, projection : 'T -> 'Key, takeCount : int) : CloudFlow<'T> = CloudFlow.sortByDescending projection takeCount this
    static member inline sortByLocal (this : CloudFlow<'T>, projection : 'T -> Local<'Key>, takeCount : int) : CloudFlow<'T> = CloudFlow.sortByLocal projection takeCount this
    static member inline sortByUsingLocal (this : CloudFlow<'T>, projection : 'T -> Local<'Key>, comparer : IComparer<'Key>, takeCount : int) : CloudFlow<'T> = CloudFlow.sortByUsingLocal projection comparer takeCount this
    static member inline sortByDescendingLocal (this : CloudFlow<'T>, projection : 'T -> Local<'Key>, takeCount : int) : CloudFlow<'T> = CloudFlow.sortByDescendingLocal projection takeCount this

    static member inline tryFind (this : CloudFlow<'T>, predicate : 'T -> bool): Cloud<'T option> = CloudFlow.tryFind predicate this
    static member inline tryFindLocal (this : CloudFlow<'T>, predicate : 'T -> Local<bool>) : Cloud<'T option> = CloudFlow.tryFindLocal predicate this

    static member inline find (this : CloudFlow<'T>, predicate : 'T -> bool) : Cloud<'T> = CloudFlow.find predicate this
    static member inline findLocal (this : CloudFlow<'T>, predicate : 'T -> Local<bool>) : Cloud<'T> = CloudFlow.findLocal predicate this

    static member inline tryPick (this : CloudFlow<'T>, chooser : 'T -> 'R option) : Cloud<'R option> = CloudFlow.tryPick chooser this
    static member inline tryPickLocal (this : CloudFlow<'T>, chooser : 'T -> Local<'R option>) : Cloud<'R option> = CloudFlow.tryPickLocal chooser this
    static member inline pick (this : CloudFlow<'T>, chooser : 'T -> 'R option) : Cloud<'R> = CloudFlow.pick chooser this
    static member inline pickLocal (this : CloudFlow<'T>, chooser : 'T -> Local<'R option>) : Cloud<'R> = CloudFlow.pickLocal chooser this

    static member inline exists (this : CloudFlow<'T>, predicate : 'T -> bool) : Cloud<bool> = CloudFlow.exists predicate this
    static member inline existsLocal (this : CloudFlow<'T>, predicate : 'T -> Local<bool>) : Cloud<bool> = CloudFlow.existsLocal predicate this

    static member inline forall (this : CloudFlow<'T>, predicate : 'T -> bool) : Cloud<bool> = CloudFlow.forall predicate this
    static member inline forallLocal (this : CloudFlow<'T>, predicate : 'T -> Local<bool>) : Cloud<bool> = CloudFlow.forallLocal predicate this

    static member inline take (this : CloudFlow<'T>, n : int) : CloudFlow<'T> = CloudFlow.take n this

    static member inline toCloudChannel (this : CloudFlow<'T>, channel : ISendPort<'T>) : Cloud<unit> = CloudFlow.toCloudChannel channel this
