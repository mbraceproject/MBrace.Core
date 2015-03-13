namespace MBrace

open System
open System.Collections
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks

[<AutoOpen>]
module Utils =

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
        let splitByPartitionCount partitions (ts : 'T []) =
            if partitions < 1 then invalidArg "partitions" "invalid number of partitions."
            elif partitions = 1 then [| ts |]
            elif partitions > ts.Length then ts |> Array.map (fun t -> [| t |])
            else
                let chunkSize = ts.Length / partitions
                let r = ts.Length % partitions
                let chunks = new ResizeArray<'T []>()
                let mutable i = 0
                for p in 0 .. partitions - 1 do
                    // add a padding element for every chunk 0 <= p < r
                    let j = i + chunkSize + if p < r then 1 else 0
                    let ch = ts.[i .. j - 1]
                    chunks.Add ch
                    i <- j

                chunks.ToArray()

        /// <summary>
        ///     partitions an array into chunks of given size.
        /// </summary>
        /// <param name="chunkSize">chunk size.</param>
        /// <param name="ts">Input array.</param>
        let splitByChunkSize chunkSize (ts : 'T []) =
            if chunkSize <= 0 then invalidArg "chunkSize" "must be positive."
            elif chunkSize > ts.Length then invalidArg "chunkSize" "chunk size greater than array size."
            let q, r = ts.Length / chunkSize , ts.Length % chunkSize
            let chunks = new ResizeArray<'T []>()
            let mutable i = 0
            for c = 0 to q - 1 do
                let j = i + chunkSize
                let ch = ts.[i .. j - 1] in chunks.Add ch
                i <- j

            if r > 0 then let ch = ts.[i .. ] in chunks.Add ch

            chunks.ToArray()

        /// <summary>
        ///     Partitions an array into chunks according to a weighted array.
        /// </summary>
        /// <param name="weights">Weights for each chunk.</param>
        /// <param name="ts">Input array.</param>
        let splitWeighted (weights : int []) (ts : 'T []) : 'T [][] =
            if ts.Length = 0 then [||] else
            if weights.Length = 0 then invalidArg "weights" "must be non-empty array."

            // compute weighted chunk sizes
            // 1. compute total = Σ w_i
            // 2. compute x_i, where x_i / N = w_i / Σ w_i
            // 3. compute R = N - Σ (floor x_i), the number of padding elements.
            // 4. compute chunk sizes, adding an extra padding element to the first R x_i's of largest decimal component.
            let N = ts.Length
            let total = weights |> Array.sumBy (fun w -> if w > 0 then w else invalidArg "weights" "weights must contain positive values.") |> uint64
            let chunkInfo = weights |> Array.map (fun w -> let C = uint64 w * uint64 N in int (C / total), int (C % total))
            let R = N - (chunkInfo |> Array.sumBy fst)
            let chunkSizes = 
                chunkInfo 
                |> Seq.mapi (fun i (q,r) -> i,q,r) 
                |> Seq.sortBy (fun (_,_,r) -> -r)
                |> Seq.mapi (fun j (i,q,_) -> if j < R then (i, q + 1) else (i,q))
                |> Seq.sortBy fst
                |> Seq.map snd
                |> Seq.toArray

            let mutable i = 0
            let chunks = new ResizeArray<'T []> ()
            for chunkSize in chunkSizes do
                let chunk = ts.[i .. i + chunkSize - 1]
                chunks.Add chunk
                i <- i + chunkSize

            chunks.ToArray()

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
                let timer = new Timer(timerCallBack, null, timeoutMilliseconds, Timeout.Infinite)
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
                let timer = new Timer(timerCallBack, null, timeoutMilliseconds, Timeout.Infinite)
                ()

            tcs.Task