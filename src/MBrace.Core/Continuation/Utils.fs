namespace MBrace

open System
open System.Collections
open System.Collections.Generic
open System.Threading
open System.Threading.Tasks

[<AutoOpen>]
module internal Utils =

    type AsyncBuilder with
        member ab.Bind(t : Task<'T>, cont : 'T -> Async<'S>) = ab.Bind(Async.AwaitTask t, cont)
        member ab.Bind(t : Task, cont : unit -> Async<'S>) =
            let t0 = t.ContinueWith ignore
            ab.Bind(Async.AwaitTask t0, cont)

    type Latch (init : int) =
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
        ///     This scheme assumes weights are relatively small, like the cpu core count of a worker node.
        /// </summary>
        /// <param name="weights">Weights for each chunk.</param>
        /// <param name="ts">Input array.</param>
        let splitWeighted (weights : int []) (ts : 'T []) : (int * 'T []) [] =
            // normalize weight array using gcd
            let normalize (ns : int []) =
                let rec gcd m n =
                    if m < n then gcd n m
                    elif n = 0 then m
                    else gcd n (m % n)

                let gcd = Array.fold (fun c n -> gcd n c) ns.[0] ns
                ns |> Array.map (fun n -> n / gcd)

            if weights.Length = 0 then invalidArg "weights" "must be non-empty array."
            let weights = normalize weights
            // compute the total number of 'chunks', which is the sum of normalized weights
            let total =
                let mutable c = 0
                for w in weights do
                    if w <= 0 then invalidArg "weights" "must contain positive weights."
                    c <- c + w
                c

            let chunkSize = ts.Length / total
            let r = ts.Length % total
            let ra = new ResizeArray<int * 'T []> ()
            let mutable i = 0 // source array index
            let mutable c = 0 // chunk count
            for w = 0 to weights.Length - 1 do
                let weigh = weights.[w]
                let l = 
                    if c + weigh <= r then weigh
                    elif c < r then r - c
                    else 0

                let j = i + chunkSize * weigh + l
                if i <= j - 1 then
                    let chunk = ts.[i .. j - 1]
                    ra.Add (w, chunk)
                i <- j
                c <- c + weigh

            ra.ToArray()
            

    [<RequireQualifiedAccess>]
    module List =

        /// <summary>
        ///     split list at given length
        /// </summary>
        /// <param name="n">splitting point.</param>
        /// <param name="xs">input list.</param>
        let splitAt n (xs : 'a list) =
            let rec splitter n (left : 'a list) right =
                match n, right with
                | 0 , _ | _ , [] -> List.rev left, right
                | n , h :: right' -> splitter (n-1) (h::left) right'

            splitter n [] xs

        /// <summary>
        ///     split list in half
        /// </summary>
        /// <param name="xs">input list</param>
        let split (xs : 'a list) = splitAt (xs.Length / 2) xs


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