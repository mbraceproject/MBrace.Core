namespace Nessos.MBrace

open System.Threading.Tasks

[<AutoOpen>]
module internal Utils =

    type AsyncBuilder with
        member ab.Bind(t : Task<'T>, cont : 'T -> Async<'S>) = ab.Bind(Async.AwaitTask t, cont)
        member ab.Bind(t : Task, cont : unit -> Async<'S>) =
            let t0 = t.ContinueWith ignore
            ab.Bind(Async.AwaitTask t0, cont)

    [<RequireQualifiedAccess>]
    module Array =

        /// <summary>
        ///     partitions an array into a predetermined number of uniformly sized chunks.
        /// </summary>5 
        /// <param name="partitions">number of partitions.</param>
        /// <param name="input">Input array.</param>
        let splitByPartitionCount partitions (ts : 'T []) =
            if partitions < 1 then invalidArg "partitions" "invalid number of partitions."
            elif partitions = 1 then [| ts |]
            elif partitions > ts.Length then invalidArg "partitions" "partitions exceed array length."
            else
                let chunkSize = ts.Length / partitions
                let r = ts.Length % partitions
                [|
                    for i in 0 .. r - 1 do
                        yield ts.[i * (chunkSize + 1) .. (i + 1) * (chunkSize + 1) - 1]

                    let I = r * (chunkSize + 1)

                    for i in 0 .. partitions - r - 1 do
                        yield ts.[I + i * chunkSize .. I + (i + 1) * chunkSize - 1]
                |]

        /// <summary>
        ///     partitions an array into chunks of given size
        /// </summary>
        /// <param name="chunkSize">chunk size.</param>
        /// <param name="ts">Input array.</param>
        let splitByChunkSize chunkSize (ts : 'T []) =
            if chunkSize <= 0 then invalidArg "chunkSize" "must be positive."
            elif chunkSize > ts.Length then invalidArg "chunkSize" "chunk size greater than array size."
            let q, r = ts.Length / chunkSize , ts.Length % chunkSize
            [|
                for i in 0 .. q-1 do
                    yield ts.[ i * chunkSize .. (i + 1) * chunkSize - 1]

                if r > 0 then yield ts.[q * chunkSize .. ]
            |]

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