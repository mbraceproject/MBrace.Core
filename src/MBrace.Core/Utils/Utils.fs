namespace MBrace.Core.Internals

open System
open System.Collections
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open System.Runtime.CompilerServices

[<AutoOpen>]
module Utils =

    type internal OAttribute = System.Runtime.InteropServices.OptionalAttribute
    type internal DAttribute = System.Runtime.InteropServices.DefaultParameterValueAttribute

    /// creates new string identifier
    let mkUUID () : string = let g = Guid.NewGuid() in g.ToString()

    type Async with
        /// <summary>
        ///     TryFinally with asynchronous 'finally' body.
        /// </summary>
        /// <param name="body">Body to be executed.</param>
        /// <param name="finallyF">Finally logic.</param>
        static member TryFinally(body : Async<'T>, finallyF : Async<unit>) = async {
            let! ct = Async.CancellationToken
            return! Async.FromContinuations(fun (sc,ec,cc) ->
                let sc' (t : 'T) = Async.StartWithContinuations(finallyF, (fun () -> sc t), ec, cc, ct)
                let ec' (e : exn) = Async.StartWithContinuations(finallyF, (fun () -> ec e), ec, cc, ct)
                Async.StartWithContinuations(body, sc', ec', cc, ct))
        }

    /// Resource that can be disposed of asynchronouslys
    type IAsyncDisposable =
        /// Asynchronously disposes of resource.
        abstract Dispose : unit -> Async<unit>

    type AsyncBuilder with
        member __.Using<'T, 'U when 'T :> IAsyncDisposable>(value : 'T, bindF : 'T -> Async<'U>) : Async<'U> =
            Async.TryFinally(async { return! bindF value }, async { return! value.Dispose() })

    [<RequireQualifiedAccess>]
    module Array =

        open Operators.Checked

        /// <summary>
        ///     partitions an array into a predetermined number of uniformly sized chunks.
        /// </summary>
        /// <param name="partitions">number of partitions.</param>
        /// <param name="input">Input array.</param>
        let splitByPartitionCountRange (partitions : int) (startRange : int64) (endRange : int64) : (int64 * int64) [] =
            if startRange > endRange then raise <| new ArgumentOutOfRangeException()
            elif partitions < 1 then invalidArg "partitions" "invalid number of partitions."
            else

            let length = endRange - startRange
            if length = 0L then Array.init partitions (fun _ -> (startRange + 1L, startRange)) else

            let partitions = int64 partitions
            let chunkSize = length / partitions
            let r = length % partitions
            let ranges = new ResizeArray<int64 * int64>()
            let mutable i = startRange
            for p in 0L .. partitions - 1L do
                // add a padding element for every chunk 0 <= p < r
                let j = i + chunkSize + if p < r then 1L else 0L
                let range = (i, j - 1L)
                ranges.Add range
                i <- j

            ranges.ToArray()

        /// <summary>
        ///     partitions an array into a predetermined number of uniformly sized chunks.
        /// </summary>
        /// <param name="partitions">number of partitions.</param>
        /// <param name="input">Input array.</param>
        let splitByPartitionCount partitions (input : 'T []) : 'T [][] =
            if obj.ReferenceEquals(input, null) then raise <| new ArgumentNullException("input")
            else
                splitByPartitionCountRange partitions 0L (int64 input.Length)
                |> Array.map (fun (s,e) -> input.[int s .. int e])

        /// <summary>
        ///     partitions an array into chunks of given size.
        /// </summary>
        /// <param name="chunkSize">chunk size.</param>
        /// <param name="input">Input array.</param>
        let splitByChunkSize chunkSize (input : 'T []) : 'T [][] =
            if chunkSize <= 0 then invalidArg "chunkSize" "must be positive."
            elif input = null then raise <| new ArgumentNullException("input")
            elif chunkSize > input.Length then invalidArg "chunkSize" "chunk size greater than array size."
            let q, r = input.Length / chunkSize , input.Length % chunkSize
            let chunks = new ResizeArray<'T []>()
            let mutable i = 0
            for c = 1 to q do
                let j = i + chunkSize
                let ch = input.[i .. j - 1] in chunks.Add ch
                i <- j

            if r > 0 then let ch = input.[i .. ] in chunks.Add ch

            chunks.ToArray()

        /// <summary>
        ///     Partitions an array into chunks according to a weighted array.
        /// </summary>
        /// <param name="weights">Weights for each chunk.</param>
        /// <param name="lower">Lower bound of range.</param>
        /// <param name="upper">Upper bound of range.</param>
        let splitWeightedRange (weights : int []) (lower : int64) (upper : int64) : (int64 * int64) option [] =
            if lower > upper then Array.init weights.Length (fun _ -> None)
            elif weights.Length = 0 then invalidArg "weights" "must be non-empty array."
            elif weights |> Array.exists (fun w -> w < 0) then invalidArg "weights" "weights must be non-negative."
            elif weights |> Array.forall (fun w -> w = 0) then invalidArg "weights" "must contain at least one positive weight."
            else

            let length = upper - lower
            if length = 0L then [| for _ in weights -> None |] 
            else

            // Weight normalization; normalize weights in order to avoid overflows
            // log2 weight + log2 length < 60 => weight * length < Int64.MaxValue
            let maxWeight = Array.max weights |> int64
            let rec aux i n =
                let log2 n = log (float n) / log 2.
                if log2 maxWeight + log2 n >= 60. then
                    aux (2 * i) (n / 2L)
                else
                    i

            let d = aux 1 length
            let weights = weights |> Array.map (fun w -> w / d)

            // compute weighted chunk sizes
            // 1. compute total = Σ w_i
            // 2. compute x_i, where x_i / N = w_i / Σ w_i
            // 3. compute R = N - Σ (floor x_i), the number of padding elements.
            // 4. compute chunk sizes, adding an extra padding element to the first R x_i's of largest decimal component.
            let total = weights |> Array.sumBy uint64
            let chunkInfo = weights |> Array.map (fun w -> let C = uint64 w * uint64 length in int64 (C / total), int64 (C % total))
            let R = length - (chunkInfo |> Array.sumBy fst)
            let chunkSizes = 
                chunkInfo 
                |> Seq.mapi (fun i (q,r) -> i,q,r) 
                |> Seq.sortBy (fun (_,_,r) -> -r)
                |> Seq.mapi (fun j (i,q,r) -> if int64 j < R && q + r > 0L then (i, q + 1L) else (i,q))
                |> Seq.sortBy fst
                |> Seq.map snd
                |> Seq.toArray

            let mutable i = lower
            let chunks = new ResizeArray<(int64 * int64) option> ()
            for chunkSize in chunkSizes do
                let range =
                    if chunkSize = 0L then None
                    else Some (i, i + chunkSize - 1L)
                chunks.Add range
                i <- i + chunkSize

            chunks.ToArray()
            

        /// <summary>
        ///     Partitions an array into chunks according to a weighted array.
        /// </summary>
        /// <param name="weights">Weights for each chunk.</param>
        /// <param name="input">Input array.</param>
        let splitWeighted (weights : int []) (input : 'T []) : 'T [][] =
            if obj.ReferenceEquals(input, null) then raise <| new ArgumentNullException("input")
            else
                splitWeightedRange weights 0L (int64 input.Length)
                |> Array.map (function None -> [||] | Some (s,e) -> input.[int s .. int e])

    [<RequireQualifiedAccess>]
    module Seq =

        /// <summary>
        ///     Creates an anonymous IEnumerable instance from an enumerator factory.
        /// </summary>
        /// <param name="enum">Enumerator factory.</param>
        let fromEnumerator (enum : unit -> IEnumerator<'T>) =
            { new IEnumerable<'T> with
                member __.GetEnumerator () = enum () 
                member __.GetEnumerator () = enum () :> IEnumerator }
        
        /// <summary>
        ///     Groups a collection of elements by key, grouping together
        ///     matching keys as long as they are sequential occurrences.
        /// </summary>
        /// <param name="proj">Key projection.</param>
        /// <param name="ts">Input sequence.</param>
        let groupBySequential (proj : 'T -> 'K) (ts : seq<'T>) : ('K * 'T []) [] =
            use enum = ts.GetEnumerator()
            let results = new ResizeArray<'K * 'T []> ()
            let grouped = new ResizeArray<'T> ()
            let mutable isFirst = true
            let mutable curr = Unchecked.defaultof<'K>
            while enum.MoveNext() do
                let t = enum.Current
                let k = proj t
                if isFirst then 
                    isFirst <- false
                    curr <- k
                    grouped.Add t
                elif k = curr then
                    grouped.Add t
                else
                    let ts = grouped.ToArray()
                    grouped.Clear()
                    results.Add(curr, ts)
                    curr <- k
                    grouped.Add t

            if grouped.Count > 0 then
                let ts = grouped.ToArray()
                results.Add(curr, ts)

            results.ToArray()

    type Task<'T> with

        /// <summary>
        ///     Create a new task that times out after a given amount of milliseconds.
        /// </summary>
        /// <param name="timeoutMilliseconds">Timeout in milliseconds.</param>
        member t.WithTimeout(timeoutMilliseconds:int) : Task<'T> =
            if timeoutMilliseconds = Timeout.Infinite then t
            else
                let tcs = new TaskCompletionSource<'T>()
                let onCompletion (t : Task<'T>) =
                    match t.Status with
                    | TaskStatus.Faulted -> tcs.TrySetException t.Exception.InnerExceptions |> ignore
                    | TaskStatus.Canceled -> tcs.TrySetCanceled () |> ignore
                    | _ -> tcs.TrySetResult t.Result |> ignore

                let timerCallBack _ = tcs.TrySetException(new TimeoutException() :> exn) |> ignore

                let _ = t.ContinueWith(onCompletion, TaskContinuationOptions.None)
                let _ = new Timer(timerCallBack, null, timeoutMilliseconds, Timeout.Infinite)
                tcs.Task

        /// <summary>
        ///     Create a new task that times out after a given amount of milliseconds.
        /// </summary>
        /// <param name="timeoutMilliseconds">Timeout in milliseconds.</param>
        member t.TryWithTimeout(timeoutMilliseconds:int) : Task<'T option> =
            let tcs = new TaskCompletionSource<'T option>()
            let onCompletion (t : Task<'T>) =
                if t.IsCompleted then tcs.TrySetResult (Some t.Result) |> ignore
                elif t.IsCanceled then tcs.TrySetCanceled () |> ignore
                elif t.IsFaulted then
                    tcs.TrySetException (t.Exception.InnerExceptions.[0]) |> ignore

            let _ = t.ContinueWith onCompletion

            if timeoutMilliseconds <> Timeout.Infinite then
                let timerCallBack _ = tcs.TrySetResult None |> ignore
                let _ = new Timer(timerCallBack, null, timeoutMilliseconds, Timeout.Infinite)
                ()

            tcs.Task

    /// A struct that can either contain a type or an exception.
    [<Struct; AutoSerializable(true); NoEquality; NoComparison>]
    type ValueOrException<'T> =
        val private isValue : bool
        val private value : 'T
        val private exn : exn

        member x.IsValue = x.isValue
        member x.IsException = not x.isValue
        member x.Value = if x.isValue then x.value else invalidOp "not a value."
        member x.Exception = if x.isValue then invalidOp "not an exception." else x.exn

        private new (isValue : bool, value : 'T, exn : exn) = { isValue = isValue ; value = value ; exn = exn }

        static member NewValue(value : 'T) = new ValueOrException<'T>(true, value, null)
        static member NewException(exn : exn) = new ValueOrException<'T>(false, Unchecked.defaultof<'T>, exn)

    module ValueOrException =
        /// Creates a new wrapper for given value.
        let inline Value(value : 'T) = ValueOrException<'T>.NewValue(value)
        /// Creates a new wrapper for given exception.
        let inline Exception(exn : exn) = ValueOrException<'T>.NewException(exn)
        /// Applies function to given argument, protecting in case of exception.
        let inline protect (f : 'T -> 'S) (t : 'T) =
            try f t |> ValueOrException<'S>.NewValue
            with e -> ValueOrException<'S>.NewException e

        let inline bind (f : 'T -> 'S) (input : ValueOrException<'T>) =
            if input.IsValue then protect f input.Value
            else ValueOrException<'S>.NewException(input.Exception)


namespace MBrace.Core.Internals.CSharpProxy

    /// C# friendly wrapper functions for F# lambdas
    type FSharpFunc =
        static member Create<'a,'b> (func:System.Converter<'a,'b>) : 'a -> 'b = fun x -> func.Invoke(x)
        static member Create<'a>(func:System.Predicate<'a>) : 'a -> bool = fun x -> func.Invoke(x)

        static member Create<'a> (func:System.Func<'a>) : unit -> 'a = fun () -> func.Invoke()
        static member Create<'a,'b> (func:System.Func<'a,'b>) : 'a -> 'b = fun x -> func.Invoke x
        static member Create<'a,'b,'c> (func:System.Func<'a,'b,'c>) : 'a -> 'b -> 'c = fun x y -> func.Invoke(x,y)
        static member Create<'a,'b,'c,'d> (func:System.Func<'a,'b,'c,'d>) : 'a -> 'b -> 'c -> 'd = fun x y z -> func.Invoke(x,y,z)
        static member Create<'a,'b,'c,'d,'e> (func:System.Func<'a,'b,'c,'d,'e>) : 'a -> 'b -> 'c -> 'd -> 'e = fun x y z w -> func.Invoke(x,y,z,w)

        static member Create(func:System.Action) : unit -> unit = fun () -> func.Invoke()
        static member Create<'a>(func:System.Action<'a>) : 'a -> unit = fun x -> func.Invoke x
        static member Create<'a,'b>(func:System.Action<'a,'b>) : 'a -> 'b -> unit = fun x y -> func.Invoke(x,y)
        static member Create<'a,'b,'c>(func:System.Action<'a,'b,'c>) : 'a -> 'b -> 'c -> unit = fun x y z -> func.Invoke(x,y,z)
        static member Create<'a,'b,'c,'d>(func:System.Action<'a,'b,'c,'d>) : 'a -> 'b -> 'c -> 'd -> unit = fun x y z w -> func.Invoke(x,y,z,w)