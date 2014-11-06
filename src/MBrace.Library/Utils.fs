namespace Nessos.MBrace.Library

[<AutoOpen>]
module internal Utils =

    [<RequireQualifiedAccess>]
    module Array =

        /// <summary>
        ///     partitions an array into a predetermined number of approximately equally sized chunks.
        /// </summary>
        /// <param name="partitions">number of partitions.</param>
        /// <param name="input">Input array.</param>
        let partition partitions (ts : 'T []) =
            if partitions < 1 then invalidArg "partitions" "invalid number of partitions."
            elif partitions = 1 then [| ts |]
            elif partitions > ts.Length then invalidArg "partitions" "partitions exceed array length."
            else
                let chunkSize = ts.Length / partitions
                let rem = ts.Length % partitions
                let I = rem * (chunkSize + 1)
                [|
                    for i in 0 .. rem - 1 do
                        yield ts.[i * (chunkSize + 1) .. (i + 1) * (chunkSize + 1) - 1]

                    for i in 0 .. partitions - rem - 1 do
                        yield ts.[I + i * chunkSize .. I + (i + 1) * chunkSize - 1]
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