namespace MBrace.Flow.Internals

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


// Taken from FSharp.Core
//
// The CLI implementation of mscorlib optimizes array sorting
// when the comparer is either null or precisely
// reference-equals to System.Collections.Generic.Comparer<'T>.Default.
// This is an indication that a "fast" array sorting helper can be used.
//
// This type is only public because of the excessive inlining used in this file
type _PrivateFastGenericComparerTable<'T when 'T : comparison>() = 

    static let fCanBeNull : System.Collections.Generic.IComparer<'T>  = 
        match typeof<'T> with 
        | ty when ty.Equals(typeof<byte>)       -> null    
        | ty when ty.Equals(typeof<char>)       -> null    
        | ty when ty.Equals(typeof<sbyte>)      -> null     
        | ty when ty.Equals(typeof<int16>)      -> null    
        | ty when ty.Equals(typeof<int32>)      -> null    
        | ty when ty.Equals(typeof<int64>)      -> null    
        | ty when ty.Equals(typeof<uint16>)     -> null    
        | ty when ty.Equals(typeof<uint32>)     -> null    
        | ty when ty.Equals(typeof<uint64>)     -> null    
        | ty when ty.Equals(typeof<float>)      -> null    
        | ty when ty.Equals(typeof<float32>)    -> null    
        | ty when ty.Equals(typeof<decimal>)    -> null    
        | _ -> LanguagePrimitives.FastGenericComparer<'T>

    static member ValueCanBeNullIfDefaultSemantics : System.Collections.Generic.IComparer<'T> = fCanBeNull