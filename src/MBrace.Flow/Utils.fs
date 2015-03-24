namespace MBrace.Flow

open System
open System.Threading
open System.Threading.Tasks

[<AutoOpen>]
module internal Utils =

    type Async with
        static member AwaitTask(t : Task) = Async.AwaitTask(t.ContinueWith(ignore, TaskContinuationOptions.None))


module internal Partitions =

    let ofLongRange (n : int) (length : int64) : (int64 * int64) []  = 
        let n = int64 n
        [| 
            for i in 0L .. n - 1L ->
                let i, j = length * i / n, length * (i + 1L) / n in (i, j) 
        |]

    let ofRange (totalWorkers : int) (length : int) : (int * int) [] = 
        ofLongRange totalWorkers (int64 length)
        |> Array.map (fun (s,e) -> int s, int e)

    let ofArray (totalWorkers : int) (array : 'T []) : 'T [] [] =
        ofRange totalWorkers array.Length
        |> Array.map (fun (s,e) -> Array.sub array s (e-s))