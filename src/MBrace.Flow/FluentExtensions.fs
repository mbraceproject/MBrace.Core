namespace MBrace.Flow.Fluent

open System.Collections.Generic
open MBrace
open MBrace.Core
open MBrace.Store
open MBrace.Flow

[<System.Runtime.CompilerServices.Extension>]
type CloudFlowExtensions() =
    /// <summary>Wraps array as a CloudFlow.</summary>
    /// <param name="this">The input array.</param>
    /// <returns>The result CloudFlow.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline toCloudFlow (this : 'T []) : CloudFlow<'T> = CloudFlow.OfArray this

    /// <summary>Transforms each element of the input CloudFlow.</summary>
    /// <param name="this">The input CloudFlow.</param>
    /// <param name="f">A function to transform items from the input CloudFlow.</param>
    /// <returns>The result CloudFlow.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline map (this : CloudFlow<'T>, f : 'T -> 'R) : CloudFlow<'R> = CloudFlow.map f this

    /// <summary>Transforms each element of the input CloudFlow using a locally executing cloud function.</summary>
    /// <param name="this">The input CloudFlow.</param>
    /// <param name="f">A locally executing cloud function to transform items from the input CloudFlow.</param>
    /// <returns>The result CloudFlow.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline mapLocal (this : CloudFlow<'T>, f : 'T -> Local<'R>) : CloudFlow<'R> = CloudFlow.mapLocal f this

    /// <summary>Transforms each element of the input CloudFlow to a new sequence and flattens its elements.</summary>
    /// <param name="this">The input CloudFlow.</param>
    /// <param name="f">A function to transform items from the input CloudFlow.</param>
    /// <returns>The result CloudFlow.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline collect (this : CloudFlow<'T>, f : 'T -> #seq<'R>) : CloudFlow<'R> = CloudFlow.collect f this

    /// <summary>Transforms each element of the input CloudFlow to a new sequence and flattens its elements using a locally executing cloud function.</summary>
    /// <param name="this">The input CloudFlow.</param>
    /// <param name="f">A locally executing cloud function to transform items from the input CloudFlow.</param>
    /// <returns>The result CloudFlow.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline collectLocal (this : CloudFlow<'T>, f : 'T -> Local<#seq<'R>>) : CloudFlow<'R> = CloudFlow.collectLocal f this

    /// <summary>Filters the elements of the input CloudFlow.</summary>
    /// <param name="this">The input CloudFlow.</param>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <returns>The result CloudFlow.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline filter (this : CloudFlow<'T>, predicate : 'T -> bool) : CloudFlow<'T> = CloudFlow.filter predicate this

    /// <summary>Filters the elements of the input CloudFlow using a locally executing cloud function.</summary>
    /// <param name="this">The input CloudFlow.</param>
    /// <param name="predicate">A locally executing cloud function to test each source element for a condition.</param>
    /// <returns>The result CloudFlow.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline filterLocal (this : CloudFlow<'T>, predicate : 'T -> Local<bool>) : CloudFlow<'T> = CloudFlow.filterLocal predicate this

    /// <summary>Returns a cloud flow with a new degree of parallelism.</summary>
    /// <param name="this">The input cloud flow.</param>
    /// <param name="degreeOfParallelism">The degree of parallelism.</param>
    /// <returns>The result cloud flow.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline withDegreeOfParallelism (this : CloudFlow<'T>, degreeOfParallelism : int) : CloudFlow<'T> = CloudFlow.withDegreeOfParallelism degreeOfParallelism this

    /// <summary>Applies a function to each element of the CloudFlow, threading an accumulator argument through the computation. If the input function is f and the elements are i0...iN, then this function computes f (... (f s i0)...) iN.</summary>
    /// <param name="this">The input CloudFlow.</param>
    /// <param name="folder">A function that updates the state with each element from the CloudFlow.</param>
    /// <param name="combiner">A function that combines partial states into a new state.</param>
    /// <param name="state">A function that produces the initial state.</param>
    /// <returns>The final result.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline foldLocal (this : CloudFlow<'T>,
                                    folder : 'State -> 'T -> Local<'State>,
                                    combiner : 'State -> 'State -> Local<'State>,
                                    state : unit -> Local<'State>) : Cloud<'State> =
        CloudFlow.foldLocal folder combiner state this

    /// <summary>Applies a function to each element of the CloudFlow, threading an accumulator argument through the computation. If the input function is f and the elements are i0...iN, then this function computes f (... (f s i0)...) iN.</summary>
    /// <param name="this">The input CloudFlow.</param>
    /// <param name="folder">A function that updates the state with each element from the CloudFlow.</param>
    /// <param name="combiner">A function that combines partial states into a new state.</param>
    /// <param name="state">A function that produces the initial state.</param>
    /// <returns>The final result.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline fold (this : CloudFlow<'T>, folder : 'State -> 'T -> 'State, combiner : 'State -> 'State -> 'State, state : unit -> 'State) = CloudFlow.fold folder combiner state this

    /// <summary>Applies a key-generating function to each element of a CloudFlow and return a CloudFlow yielding unique keys and the result of the threading an accumulator. The folder, combiner and state are locally executing cloud functions.</summary>
    /// <param name="this">The input CloudFlow.</param>
    /// <param name="projection">A function to transform items from the input CloudFlow to keys.</param>
    /// <param name="folder">A locally executing cloud function that updates the state with each element from the CloudFlow.</param>
    /// <param name="combiner">A locally executing cloud function that combines partial states into a new state.</param>
    /// <param name="state">A locally executing cloud function that produces the initial state.</param>
    /// <returns>The final result.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline foldByLocal (this : CloudFlow<'T>,
                                      projection : 'T -> Local<'Key>,
                                      folder : 'State -> 'T -> Local<'State>,
                                      combiner : 'State -> 'State -> Local<'State>,
                                      state : unit -> Local<'State>)  : CloudFlow<'Key * 'State> =
        CloudFlow.foldByLocal projection folder combiner state this

    /// <summary>Applies a key-generating function to each element of a CloudFlow and return a CloudFlow yielding unique keys and the result of the threading an accumulator.</summary>
    /// <param name="this">The input CloudFlow.</param>
    /// <param name="projection">A function to transform items from the input CloudFlow to keys.</param>
    /// <param name="folder">A function that updates the state with each element from the CloudFlow.</param>
    /// <param name="combiner">A function that combines partial states into a new state.</param>
    /// <param name="state">A function that produces the initial state.</param>
    /// <returns>The final result.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline foldBy (this : CloudFlow<'T>,
                                 projection : 'T -> 'Key,
                                 folder : 'State -> 'T -> 'State,
                                 combiner : 'State -> 'State -> 'State,
                                 state : unit -> 'State) : CloudFlow<'Key * 'State> =
        CloudFlow.foldBy projection folder combiner state this

    /// <summary>
    /// Applies a key-generating function to each element of a CloudFlow and return a CloudFlow yielding unique keys and their number of occurrences in the original sequence.
    /// </summary>
    /// <param name="flow">The input CloudFlow.</param>
    /// <param name="projection">A function that maps items from the input CloudFlow to keys.</param>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline countBy (this : CloudFlow<'T>, projection : 'T -> 'Key) : CloudFlow<'Key * int64> = CloudFlow.countBy projection this

    /// <summary>
    /// Applies a key-generating function to each element of a CloudFlow and return a CloudFlow yielding unique keys and their number of occurrences in the original sequence.
    /// </summary>
    /// <param name="this">The input CloudFlow.</param>
    /// <param name="projection">A function that maps items from the input CloudFlow to keys.</param>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline countByLocal (this : CloudFlow<'T>, projection : 'T -> Local<'Key>) : CloudFlow<'Key * int64> = CloudFlow.countByLocal projection this

    /// <summary>Runs the action on each element. The actions are not necessarily performed in order.</summary>
    /// <param name="this">The input CloudFlow.</param>
    /// <returns>Nothing.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline iter (this : CloudFlow<'T>, action: 'T -> unit) : Cloud< unit > = CloudFlow.iter action this

    /// <summary>Runs the action on each element. The actions are not necessarily performed in order.</summary>
    /// <param name="this">The input CloudFlow.</param>
    /// <returns>Nothing.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline iterLocal (this : CloudFlow<'T>, action: 'T -> Local<unit>) : Cloud<unit> = CloudFlow.iterLocal action this

    /// <summary>Returns the sum of the elements.</summary>
    /// <param name="this">The input CloudFlow.</param>
    /// <returns>The sum of the elements.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline sum (flow : CloudFlow< ^T >) : Cloud< ^T >
        when ^T : (static member ( + ) : ^T * ^T -> ^T)
        and  ^T : (static member Zero : ^T) =
            CloudFlow.sum flow

    /// <summary>Applies a key-generating function to each element of a CloudFlow and return the sum of the keys.</summary>
    /// <param name="this">The input CloudFlow.</param>
    /// <returns>The sum of the keys.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline sumBy (this : CloudFlow<'T>, f : 'T -> ^S) : Cloud< ^S >
        when ^S : (static member ( + ) : ^S * ^S -> ^S)
        and  ^S : (static member Zero : ^S) =
            CloudFlow.sumBy f this

    /// <summary>Applies a key-generating locally executing cloud function to each element of a CloudFlow and return the sum of the keys.</summary>
    /// <param name="this">The input CloudFlow.</param>
    /// <returns>The sum of the keys.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline sumByLocal (this : CloudFlow<'T>, f : 'T -> Local< ^S >) : Cloud< ^S >
        when ^S : (static member ( + ) : ^S * ^S -> ^S)
        and  ^S : (static member Zero : ^S) =
            CloudFlow.sumByLocal f this


    /// <summary>Returns the total number of elements of the CloudFlow.</summary>
    /// <param name="this">The input CloudFlow.</param>
    /// <returns>The total number of elements.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline length (this : CloudFlow<'T>) : Cloud<int64> = CloudFlow.length this

    /// <summary>Creates an array from the given CloudFlow.</summary>
    /// <param name="this">The input CloudFlow.</param>
    /// <returns>The result array.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline toArray (this : CloudFlow<'T>) : Cloud<'T[]> = CloudFlow.toArray this

    /// <summary>Creates a PersistedCloudFlow from the given CloudFlow.</summary>
    /// <param name="this">The input CloudFlow.</param>
    /// <returns>The result PersistedCloudFlow.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline persist (this : CloudFlow<'T>) : Cloud<PersistedCloudFlow<'T>> = CloudFlow.persist this

    /// <summary>Creates a PersistedCloudFlow from the given CloudFlow, with its partitions cached locally</summary>
    /// <param name="this">The input CloudFlow.</param>
    /// <returns>The result PersistedCloudFlow.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline persistCached (this : CloudFlow<'T>) : Cloud<PersistedCloudFlow<'T>> = CloudFlow.persistCached this

    /// <summary>Applies a key-generating function to each element of the input CloudFlow and yields the CloudFlow of the given length, ordered by keys.</summary>
    /// <param name="this">The input CloudFlow.</param>
    /// <param name="projection">A function to transform items of the input CloudFlow into comparable keys.</param>
    /// <param name="takeCount">The number of elements to return.</param>
    /// <returns>The result CloudFlow.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline sortBy (this : CloudFlow<'T>, projection : 'T -> 'Key, takeCount : int) : CloudFlow<'T> = CloudFlow.sortBy projection takeCount this

    /// <summary>Applies a key-generating function to each element of the input CloudFlow and yields the CloudFlow of the given length, ordered using the given comparer for the keys.</summary>
    /// <param name="this">The input CloudFlow.</param>
    /// <param name="projection">A function to transform items of the input CloudFlow into comparable keys.</param>
    /// <param name="takeCount">The number of elements to return.</param>
    /// <returns>The result CloudFlow.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline sortByUsing (this : CloudFlow<'T>, projection : 'T -> 'Key, comparer : IComparer<'Key>, takeCount : int) : CloudFlow<'T> = CloudFlow.sortByUsing projection comparer takeCount this

    /// <summary>Applies a key-generating function to each element of the input CloudFlow and yields the CloudFlow of the given length, ordered descending by keys.</summary>
    /// <param name="this">The input CloudFlow.</param>
    /// <param name="projection">A function to transform items of the input CloudFlow into comparable keys.</param>
    /// <param name="takeCount">The number of elements to return.</param>
    /// <returns>The result CloudFlow.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline sortByDescending (this : CloudFlow<'T>, projection : 'T -> 'Key, takeCount : int) : CloudFlow<'T> = CloudFlow.sortByDescending projection takeCount this

    /// <summary>Applies a key-generating locally executing cloud function to each element of the input CloudFlow and yields the CloudFlow of the given length, ordered by keys.</summary>
    /// <param name="this">The input CloudFlow.</param>
    /// <param name="projection">A locally executing cloud function to transform items of the input CloudFlow into comparable keys.</param>
    /// <param name="takeCount">The number of elements to return.</param>
    /// <returns>The result CloudFlow.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline sortByLocal (this : CloudFlow<'T>, projection : 'T -> Local<'Key>, takeCount : int) : CloudFlow<'T> = CloudFlow.sortByLocal projection takeCount this

    /// <summary>Applies a key-generating locally executing cloud function to each element of the input CloudFlow and yields the CloudFlow of the given length, ordered by keys.</summary>
    /// <param name="this">The input CloudFlow.</param>
    /// <param name="projection">A locally executing cloud function to transform items of the input CloudFlow into comparable keys.</param>
    /// <param name="takeCount">The number of elements to return.</param>
    /// <returns>The result CloudFlow.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline sortByUsingLocal (this : CloudFlow<'T>, projection : 'T -> Local<'Key>, comparer : IComparer<'Key>, takeCount : int) : CloudFlow<'T> = CloudFlow.sortByUsingLocal projection comparer takeCount this

    /// <summary>Applies a key-generating locally executing cloud function to each element of the input CloudFlow and yields the CloudFlow of the given length, ordered by descending keys.</summary>
    /// <param name="this">The input CloudFlow.</param>
    /// <param name="projection">A locally executing cloud function to transform items of the input CloudFlow into comparable keys.</param>
    /// <param name="takeCount">The number of elements to return.</param>
    /// <returns>The result CloudFlow.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline sortByDescendingLocal (this : CloudFlow<'T>, projection : 'T -> Local<'Key>, takeCount : int) : CloudFlow<'T> = CloudFlow.sortByDescendingLocal projection takeCount this

    /// <summary>Returns the first element for which the given function returns true. Returns None if no such element exists.</summary>
    /// <param name="this">The input cloud flow.</param>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <returns>The first element for which the predicate returns true, or None if every element evaluates to false.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline tryFind (this : CloudFlow<'T>, predicate : 'T -> bool): Cloud<'T option> = CloudFlow.tryFind predicate this

    /// <summary>Returns the first element for which the given locally executing cloud function returns true. Returns None if no such element exists.</summary>
    /// <param name="thisg">The input cloud flow.</param>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <returns>The first element for which the predicate returns true, or None if every element evaluates to false.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline tryFindLocal (this : CloudFlow<'T>, predicate : 'T -> Local<bool>) : Cloud<'T option> = CloudFlow.tryFindLocal predicate this

    /// <summary>Returns the first element for which the given function returns true. Raises KeyNotFoundException if no such element exists.</summary>
    /// <param name="this">The input cloud flow.</param>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <returns>The first element for which the predicate returns true.</returns>
    /// <exception cref="System.KeyNotFoundException">Thrown if the predicate evaluates to false for all the elements of the cloud flow.</exception>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline find (this : CloudFlow<'T>, predicate : 'T -> bool) : Cloud<'T> = CloudFlow.find predicate this

    /// <summary>Returns the first element for which the given locally executing cloud function returns true. Raises KeyNotFoundException if no such element exists.</summary>
    /// <param name="this">The input cloud flow.</param>
    /// <param name="predicate">A locally executing cloud function to test each source element for a condition.</param>
    /// <returns>The first element for which the predicate returns true.</returns>
    /// <exception cref="System.KeyNotFoundException">Thrown if the predicate evaluates to false for all the elements of the cloud flow.</exception>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline findLocal (this : CloudFlow<'T>, predicate : 'T -> Local<bool>) : Cloud<'T> = CloudFlow.findLocal predicate this

    /// <summary>Applies the given function to successive elements, returning the first result where the function returns a Some value.</summary>
    /// <param name="this">The input cloud flow.</param>
    /// <param name="chooser">A function that transforms items into options.</param>
    /// <returns>The first element for which the chooser returns Some, or None if every element evaluates to None.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline tryPick (this : CloudFlow<'T>, chooser : 'T -> 'R option) : Cloud<'R option> = CloudFlow.tryPick chooser this

    /// <summary>Applies the given locally executing cloud function to successive elements, returning the first result where the function returns a Some value.</summary>
    /// <param name="this">The input cloud flow.</param>
    /// <param name="chooser">A locally executing cloud function that transforms items into options.</param>
    /// <returns>The first element for which the chooser returns Some, or None if every element evaluates to None.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline tryPickLocal (this : CloudFlow<'T>, chooser : 'T -> Local<'R option>) : Cloud<'R option> = CloudFlow.tryPickLocal chooser this

    /// <summary>Applies the given function to successive elements, returning the first result where the function returns a Some value.
    /// Raises KeyNotFoundException when every item of the cloud flow evaluates to None when the given function is applied.</summary>
    /// <param name="this">The input cloud flow.</param>
    /// <param name="chooser">A function that transforms items into options.</param>
    /// <returns>The first element for which the chooser returns Some, or raises KeyNotFoundException if every element evaluates to None.</returns>
    /// <exception cref="System.KeyNotFoundException">Thrown if every item of the cloud flow evaluates to None when the given function is applied.</exception>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline pick (this : CloudFlow<'T>, chooser : 'T -> 'R option) : Cloud<'R> = CloudFlow.pick chooser this

    /// <summary>Applies the given locally executing cloud function to successive elements, returning the first result where the function returns a Some value.
    /// Raises KeyNotFoundException when every item of the cloud flow evaluates to None when the given function is applied.</summary>
    /// <param name="this">The input cloud flow.</param>
    /// <param name="chooser">A locally executing cloud function that transforms items into options.</param>
    /// <returns>The first element for which the chooser returns Some, or raises KeyNotFoundException if every element evaluates to None.</returns>
    /// <exception cref="System.KeyNotFoundException">Thrown if every item of the cloud flow evaluates to None when the given function is applied.</exception>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline pickLocal (this : CloudFlow<'T>, chooser : 'T -> Local<'R option>) : Cloud<'R> = CloudFlow.pickLocal chooser this

    /// <summary>Tests if any element of the flow satisfies the given predicate.</summary>
    /// <param name="this">The input cloud flow.</param>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <returns>true if any element satisfies the predicate. Otherwise, returns false.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline exists (this : CloudFlow<'T>, predicate : 'T -> bool) : Cloud<bool> = CloudFlow.exists predicate this

    /// <summary>Tests if any element of the flow satisfies the given locally executing cloud predicate.</summary>
    /// <param name="this">The input cloud flow.</param>
    /// <param name="predicate">A locally executing cloud function to test each source element for a condition.</param>
    /// <returns>true if any element satisfies the predicate. Otherwise, returns false.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline existsLocal (this : CloudFlow<'T>, predicate : 'T -> Local<bool>) : Cloud<bool> = CloudFlow.existsLocal predicate this

    /// <summary>Tests if all elements of the parallel flow satisfy the given predicate.</summary>
    /// <param name="this">The input cloud flow.</param>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <returns>true if all of the elements satisfies the predicate. Otherwise, returns false.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline forall (this : CloudFlow<'T>, predicate : 'T -> bool) : Cloud<bool> = CloudFlow.forall predicate this

    /// <summary>Tests if all elements of the parallel flow satisfy the given predicate.</summary>
    /// <param name="this">The input cloud flow.</param>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <returns>true if all of the elements satisfies the predicate. Otherwise, returns false.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline forallLocal (this : CloudFlow<'T>, predicate : 'T -> Local<bool>) : Cloud<bool> = CloudFlow.forallLocal predicate this

    /// <summary> Returns the elements of a CloudFlow up to a specified count. </summary>
    /// <param name="this">The input CloudFlow.</param>
    /// <param name="n">The maximum number of items to take.</param>
    /// <returns>The resulting CloudFlow.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline take (this : CloudFlow<'T>, n : int) : CloudFlow<'T> = CloudFlow.take n this

    /// <summary>Sends the values of CloudFlow to the SendPort of a CloudChannel</summary>
    /// <param name="this">The input CloudFlow.</param>
    /// <param name="channel">the SendPort of a CloudChannel.</param>
    /// <returns>Nothing.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline toCloudChannel (this : CloudFlow<'T>, channel : ISendPort<'T>) : Cloud<unit> = CloudFlow.toCloudChannel channel this

    /// <summary>
    ///     Returs true if the flow is empty and false otherwise.
    /// </summary>
    /// <param name="stream">The input flow.</param>
    /// <returns>true if the input flow is empty, false otherwise</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline isEmpty (this : CloudFlow<'T>) : Cloud<bool> = CloudFlow.isEmpty this


    /// <summary>Locates the maximum element of the flow by given key.</summary>
    /// <param name="this">The input flow.</param>
    /// <param name="projection">A function to transform items of the input flow into comparable keys.</param>
    /// <returns>The maximum item.</returns>
    /// <exception cref="System.ArgumentException">Thrown if the input flow is empty.</exception>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline maxBy<'T, 'Key when 'Key : comparison> (this : CloudFlow<'T>, projection : 'T -> 'Key) : Cloud<'T> =
        CloudFlow.maxBy projection this

    /// <summary>Locates the minimum element of the flow by given key.</summary>
    /// <param name="this">The input flow.</param>
    /// <param name="projection">A function to transform items of the input flow into comparable keys.</param>
    /// <returns>The minimum item.</returns>
    /// <exception cref="System.ArgumentException">Thrown if the input flow is empty.</exception>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline minBy<'T, 'Key when 'Key : comparison> (this : CloudFlow<'T>, projection : 'T -> 'Key) : Cloud<'T> =
        CloudFlow.minBy projection this

    /// <summary>
    ///    Reduces the elements of the input flow to a single value via the given reducer function.
    ///    The reducer function is first applied to the first two elements of the flow.
    ///    Then, the reducer is applied on the result of the first reduction and the third element.
    //     The process continues until all the elements of the flow have been reduced.
    /// </summary>
    /// <param name="this">The input flow.</param>
    /// <param name="reducer">The reducer function.</param>
    /// <returns>The reduced value.</returns>
    /// <exception cref="System.ArgumentException">Thrown if the input flow is empty.</exception>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline reduce (this : CloudFlow<'T>, reducer : 'T -> 'T -> 'T) : Cloud<'T> =
        CloudFlow.reduce reducer this

    /// <summary>Computes the average of the projections given by the supplied function on the input flow.</summary>
    /// <param name="this">The input flow.</param>
    /// <param name="projection">A function to transform items of the input flow into a projection.</param>
    /// <returns>The computed average.</returns>
    /// <exception cref="System.ArgumentException">Thrown if the input flow is empty.</exception>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline averageBy (this : CloudFlow<'T>, projection : 'T -> ^U) : Cloud< ^U >
            when ^U : (static member (+) : ^U * ^U -> ^U)
            and  ^U : (static member DivideByInt : ^U * int -> ^U)
            and  ^U : (static member Zero : ^U) =
        CloudFlow.averageBy projection this

    /// <summary>Computes the average of the elements in the input flow.</summary>
    /// <param name="this">The input flow.</param>
    /// <returns>The computed average.</returns>
    /// <exception cref="System.ArgumentException">Thrown if the input flow is empty.</exception>
    static member inline average (source : ParStream< ^T >) : ^T
            when ^T : (static member (+) : ^T * ^T -> ^T)
            and  ^T : (static member DivideByInt : ^T * int -> ^T)
            and  ^T : (static member Zero : ^T) =
        CloudFlow.averageBy id source
