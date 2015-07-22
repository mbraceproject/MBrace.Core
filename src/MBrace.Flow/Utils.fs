namespace MBrace.Flow.Internals

open System
open System.Threading
open System.Threading.Tasks
open System.Collections
open System.Collections.Generic

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Flow

[<AutoOpen>]
module Utils =

    type Async with
        static member AwaitTask(t : Task) = Async.AwaitTask(t.ContinueWith(ignore, TaskContinuationOptions.None))

    type Collector<'T, 'R> with
        /// Converts MBrace.Flow.Collector to Nessos.Streams.Collector
        member collector.ToParStreamCollector () =
            { new Nessos.Streams.Collector<'T, 'R> with
                member self.DegreeOfParallelism = match collector.DegreeOfParallelism with Some n -> n | None -> Environment.ProcessorCount
                member self.Iterator() = collector.Iterator()
                member self.Result = collector.Result }

    module Option =
        /// converts nullable to optional
        let ofNullable (t : Nullable<'T>) =
            if t.HasValue then Some t.Value
            else None

    module ResizeArray =
        
        [<AutoSerializable(false)>]
        type private ResizeArrayConcatenator<'T>(inputs : ResizeArray<'T> []) =
            let isEmpty = Array.isEmpty inputs
            let mutable lp = 0
            let mutable ep = -1
            let mutable l =
                if isEmpty then Unchecked.defaultof<_> 
                else inputs.[0]

            interface IEnumerator<'T> with
                member x.Current: 'T = l.[ep]
                member x.Current: obj = l.[ep] :> obj
                member x.Dispose(): unit = ()
                member x.MoveNext(): bool =
                    if isEmpty then false else
                    if ep + 1 = l.Count then
                        // shift to next List, skipping empty occurences
                        lp <- lp + 1
                        while lp < inputs.Length && inputs.[lp].Count = 0 do
                            lp <- lp + 1
                            
                        if lp = inputs.Length then false
                        else
                            l <- inputs.[lp]
                            ep <- 0
                            true
                    else
                        ep <- ep + 1
                        true
                
                member x.Reset(): unit = 
                    if isEmpty then () else
                    lp <- 0
                    l <- inputs.[0]
                    ep <- -1

        /// Ad-hoc ResizeArray concatenation combinator
        let concat (inputs : seq<ResizeArray<'T>>) : seq<'T> =
            let inputs = Seq.toArray inputs
            Seq.fromEnumerator (fun () -> new ResizeArrayConcatenator<'T>(inputs) :> _)

    module Partition =

        // TODO : property tests!

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

        /// partition elements so that size in accumulated groupings does not surpass maxSize
        let partitionBySize (getSize : 'T -> Async<int64>) (maxSize : int64) (inputs : 'T []) = async {
            let rec aux i accSize (accElems : 'T list) (accGroupings : 'T list list) = async {
                if i >= inputs.Length then return accElems :: accGroupings
                else
                    let t = inputs.[i]
                    let! size = getSize t
                    // if size of element exceeds limit, put element in grouping of its own; 
                    // note that this may affect ordering of partitioned elements
                    if size >= maxSize then return! aux (i + 1) accSize accElems ([t] :: accGroupings)
                    // accumulated length exceeds limit, flush accumulated elements to groupings and retry
                    elif accSize + size >= maxSize then return! aux i 0L [] (accElems :: accGroupings)
                    // within limit, append to accumulated elements
                    else return! aux (i + 1) (accSize + size) (t :: accElems) accGroupings
            }

            let! groupings = aux 0 0L [] []
            return
                groupings
                |> List.rev
                |> Seq.map (fun gp -> gp |> List.rev |> List.toArray)
                |> Seq.toArray
        }


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