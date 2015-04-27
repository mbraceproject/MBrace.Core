namespace MBrace.Core.Internals

open System
open System.Collections
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks

[<AutoOpen>]
module Utils =

    /// creates new string identifier
    let mkUUID () : string = let g = Guid.NewGuid() in g.ToString("N")

    type AsyncBuilder with
        member ab.Bind(t : Task<'T>, cont : 'T -> Async<'S>) = ab.Bind(Async.AwaitTask t, cont)
        member ab.Bind(t : Task, cont : unit -> Async<'S>) =
            let t0 = t.ContinueWith ignore
            ab.Bind(Async.AwaitTask t0, cont)

    type internal Latch (init : int) =
        [<VolatileField>]
        let mutable value = init

        member __.Increment() = Interlocked.Increment &value
        member __.Value = value

    [<RequireQualifiedAccess>]
    module Array =

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
            if length = 0L then [||] else

            let partitions = if length < int64 partitions then length else int64 partitions
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


        /// computes the gcd for a collection of integers
        let inline gcd (inputs : 't []) : 't =
            let rec gcd m n =
                if n > m then gcd n m
                elif n = LanguagePrimitives.GenericZero then m
                else gcd n (m % n)

            Array.fold gcd LanguagePrimitives.GenericZero inputs

        /// normalize a collection of inputs w.r.t. gcd
        let inline gcdNormalize (inputs : 't []) : 't [] =
            let gcd = gcd inputs
            inputs |> Array.map (fun i -> i / gcd)


    module Seq =
        let fromEnumerator (enum : unit -> IEnumerator<'T>) =
            { new IEnumerable<'T> with
                member __.GetEnumerator () = enum () 
                member __.GetEnumerator () = enum () :> IEnumerator }

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
                elif t.IsFaulted then tcs.TrySetException t.Exception.InnerExceptions |> ignore

            let _ = t.ContinueWith onCompletion

            if timeoutMilliseconds <> Timeout.Infinite then
                let timerCallBack _ = tcs.TrySetResult None |> ignore
                let _ = new Timer(timerCallBack, null, timeoutMilliseconds, Timeout.Infinite)
                ()

            tcs.Task