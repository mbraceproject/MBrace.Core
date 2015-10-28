namespace MBrace.Flow.Fluent

open System.Collections.Generic
open MBrace
open MBrace.Core
open MBrace.Flow

[<System.Runtime.CompilerServices.Extension>]
type CloudFlowExtensions =
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

    /// <summary>Enables the insertion of a monadic side-effect in a distributed flow. Output remains the same.</summary>
    /// <param name="f">A locally executing cloud function that performs side effect on input flow elements.</param>
    /// <returns>The result CloudFlow.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline peek (this : CloudFlow<'T>, f : 'T -> LocalCloud<unit>) : CloudFlow<'T> = CloudFlow.peek f this

    /// <summary>Transforms each element of the input CloudFlow to a new sequence and flattens its elements.</summary>
    /// <param name="this">The input CloudFlow.</param>
    /// <param name="f">A function to transform items from the input CloudFlow.</param>
    /// <returns>The result CloudFlow.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline collect (this : CloudFlow<'T>, f : 'T -> #seq<'R>) : CloudFlow<'R> = CloudFlow.collect f this

    /// <summary>Filters the elements of the input CloudFlow.</summary>
    /// <param name="this">The input CloudFlow.</param>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <returns>The result CloudFlow.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline filter (this : CloudFlow<'T>, predicate : 'T -> bool) : CloudFlow<'T> = CloudFlow.filter predicate this

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
    static member inline fold (this : CloudFlow<'T>, folder : 'State -> 'T -> 'State, combiner : 'State -> 'State -> 'State, state : unit -> 'State) = CloudFlow.fold folder combiner state this

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

    /// <summary>Runs the action on each element. The actions are not necessarily performed in order.</summary>
    /// <param name="this">The input CloudFlow.</param>
    /// <returns>Nothing.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline iter (this : CloudFlow<'T>, action: 'T -> unit) : Cloud< unit > = CloudFlow.iter action this

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
    static member inline persist (this : CloudFlow<'T>, storageLevel : StorageLevel) : Cloud<PersistedCloudFlow<'T>> = CloudFlow.persist storageLevel this

    /// <summary>Creates a PersistedCloudFlow from the given CloudFlow, with its partitions cached locally</summary>
    /// <param name="this">The input CloudFlow.</param>
    /// <returns>The result PersistedCloudFlow.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline cache (this : CloudFlow<'T>) : Cloud<PersistedCloudFlow<'T>> = CloudFlow.cache this

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

    /// <summary>Returns the first element for which the given function returns true. Returns None if no such element exists.</summary>
    /// <param name="this">The input cloud flow.</param>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <returns>The first element for which the predicate returns true, or None if every element evaluates to false.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline tryFind (this : CloudFlow<'T>, predicate : 'T -> bool): Cloud<'T option> = CloudFlow.tryFind predicate this

    /// <summary>Returns the first element for which the given function returns true. Raises KeyNotFoundException if no such element exists.</summary>
    /// <param name="this">The input cloud flow.</param>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <returns>The first element for which the predicate returns true.</returns>
    /// <exception cref="System.KeyNotFoundException">Thrown if the predicate evaluates to false for all the elements of the cloud flow.</exception>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline find (this : CloudFlow<'T>, predicate : 'T -> bool) : Cloud<'T> = CloudFlow.find predicate this

    /// <summary>Applies the given function to successive elements, returning the first result where the function returns a Some value.</summary>
    /// <param name="this">The input cloud flow.</param>
    /// <param name="chooser">A function that transforms items into options.</param>
    /// <returns>The first element for which the chooser returns Some, or None if every element evaluates to None.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline tryPick (this : CloudFlow<'T>, chooser : 'T -> 'R option) : Cloud<'R option> = CloudFlow.tryPick chooser this

    /// <summary>Applies the given function to successive elements, returning the first result where the function returns a Some value.
    /// Raises KeyNotFoundException when every item of the cloud flow evaluates to None when the given function is applied.</summary>
    /// <param name="this">The input cloud flow.</param>
    /// <param name="chooser">A function that transforms items into options.</param>
    /// <returns>The first element for which the chooser returns Some, or raises KeyNotFoundException if every element evaluates to None.</returns>
    /// <exception cref="System.KeyNotFoundException">Thrown if every item of the cloud flow evaluates to None when the given function is applied.</exception>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline pick (this : CloudFlow<'T>, chooser : 'T -> 'R option) : Cloud<'R> = CloudFlow.pick chooser this

    /// <summary>Tests if any element of the flow satisfies the given predicate.</summary>
    /// <param name="this">The input cloud flow.</param>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <returns>true if any element satisfies the predicate. Otherwise, returns false.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline exists (this : CloudFlow<'T>, predicate : 'T -> bool) : Cloud<bool> = CloudFlow.exists predicate this

    /// <summary>Tests if all elements of the parallel flow satisfy the given predicate.</summary>
    /// <param name="this">The input cloud flow.</param>
    /// <param name="predicate">A function to test each source element for a condition.</param>
    /// <returns>true if all of the elements satisfies the predicate. Otherwise, returns false.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline forall (this : CloudFlow<'T>, predicate : 'T -> bool) : Cloud<bool> = CloudFlow.forall predicate this

    /// <summary> Returns the elements of a CloudFlow up to a specified count. </summary>
    /// <param name="this">The input CloudFlow.</param>
    /// <param name="n">The maximum number of items to take.</param>
    /// <returns>The resulting CloudFlow.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline take (this : CloudFlow<'T>, n : int) : CloudFlow<'T> = CloudFlow.take n this

    /// <summary>Sends the values of CloudFlow to the SendPort of a CloudQueue</summary>
    /// <param name="this">The input CloudFlow.</param>
    /// <param name="channel">the SendPort of a CloudQueue.</param>
    /// <returns>Nothing.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline toCloudQueue (this : CloudFlow<'T>, channel : CloudQueue<'T>) : Cloud<unit> = CloudFlow.toCloudQueue channel this

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

    /// <summary>
    ///    Groups the elements of the input flow according to given key generating function
    ///    and reduces the elements of each group to a single value via the given reducer function.
    /// </summary>
    /// <param name="source">The input flow.</param>
    /// <param name="projection">A function to transform items of the input flow into a key.</param>
    /// <param name="reducer">The reducer function.</param>
    /// <returns>A flow of key - reduced value pairs.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline reduceBy (source : CloudFlow<'T>, projection : 'T -> 'Key, reducer : 'T -> 'T -> 'T) : CloudFlow<'Key * 'T> =
        CloudFlow.reduceBy projection reducer source

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
    /// <param name="source">The input flow.</param>
    /// <returns>The computed average.</returns>
    /// <exception cref="System.ArgumentException">Thrown if the input flow is empty.</exception>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline average (source : CloudFlow< ^T >) : Cloud< ^T >
            when ^T : (static member (+) : ^T * ^T -> ^T)
            and  ^T : (static member DivideByInt : ^T * int -> ^T)
            and  ^T : (static member Zero : ^T) =
        CloudFlow.averageBy id source

    /// <summary>Applies a key-generating function to each element of the input flow and yields a flow of unique keys and a sequence of all elements that have each key.</summary>
    /// <param name="source">The input flow.</param>
    /// <param name="projection">A function to transform items of the input flow into comparable keys.</param>
    /// <returns>A flow of tuples where each tuple contains the unique key and a sequence of all the elements that match the key.</returns>
    /// <remarks>
    ///     Note: This combinator may be very expensive; for example if the group sizes are expected to be large.
    ///     If you intend to perform an aggregate operation, such as sum or average,
    ///     you are advised to use CloudFlow.foldBy or CloudFlow.countBy, for much better performance.
    /// </remarks>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline groupBy (source : CloudFlow<'T>, projection : 'T -> 'Key) : CloudFlow<'Key * seq<'T>> =
        CloudFlow.groupBy projection source

    /// <summary>Returns a flow that contains no duplicate entries according to the generic hash and equality comparisons on the keys returned by the given key-generating function. If an element occurs multiple times in the flow then only one is retained.</summary>
    /// <param name="source">The input flow.</param>
    /// <param name="projection">A function to transform items of the input flow into comparable keys.</param>
    /// <returns>A flow of elements distinct on their keys.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline distinctBy (source : CloudFlow<'T>, projection : 'T -> 'Key) : CloudFlow<'T> =
        CloudFlow.distinctBy projection source

    /// <summary>Returns a flow that contains no duplicate elements according to their generic hash and equality comparisons. If an element occurs multiple times in the flow then only one is retained.</summary>
    /// <param name="source">The input flow.</param>
    /// <returns>A flow of distinct elements.</returns>
    [<System.Runtime.CompilerServices.Extension>]
    static member inline distinct (source : CloudFlow<'T>) : CloudFlow<'T> =
        CloudFlow.distinct source
