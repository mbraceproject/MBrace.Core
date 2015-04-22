namespace MBrace.Flow.Internals
open System
open System.Collections
open System.Collections.Generic
open System.Threading.Tasks
open Nessos.Streams
open MBrace.Flow
open MBrace.Core

/// [omit]
/// Proxy for FSharp type specialization and lambda inlining.
// use internalsVisibleTo for CSharp library
type internal CSharpProxy = 

    static member Select<'T, 'R> (stream : CloudFlow<'T>, func : Func<'T, 'R>) = 
        CloudFlow.map (fun x -> func.Invoke(x)) stream

    static member Where<'T> (stream : CloudFlow<'T>, func : Func<'T, bool>) = 
        CloudFlow.filter (fun x -> func.Invoke(x)) stream

    static member SelectMany<'T, 'R>(stream : CloudFlow<'T>, func : Func<'T, IEnumerable<'R>>) =
        CloudFlow.collect (fun x -> func.Invoke(x)) stream 

    static member Aggregate<'T, 'Acc>(stream : CloudFlow<'T>, state : Func<'Acc>, folder : Func<'Acc, 'T, 'Acc>, combiner : Func<'Acc, 'Acc, 'Acc>) = 
        CloudFlow.fold (fun acc x -> folder.Invoke(acc, x)) (fun left right -> combiner.Invoke(left, right)) (fun _ -> state.Invoke()) stream

    static member AggregateBy<'T, 'Key, 'Acc when 'Key : equality>(stream : CloudFlow<'T>, projection : Func<'T,'Key> , state : Func<'Acc>, folder : Func<'Acc, 'T, 'Acc>, combiner : Func<'Acc, 'Acc, 'Acc>) = 
        CloudFlow.foldBy (fun x -> projection.Invoke(x)) (fun acc x -> folder.Invoke(acc, x)) (fun left right -> combiner.Invoke(left, right)) (fun _ -> state.Invoke()) stream

    static member OrderBy<'T, 'Key when 'Key :> IComparable<'Key> and 'Key : comparison>(stream : CloudFlow<'T>, func : Func<'T, 'Key>, takeCount : int) =
        CloudFlow.sortBy (fun x -> func.Invoke(x)) takeCount stream

    static member Count<'T>(stream : CloudFlow<'T>) = 
        CloudFlow.length stream
        
    static member CountBy<'T, 'Key when 'Key : equality>(stream : CloudFlow<'T>, func : Func<'T, 'Key>) =
        CloudFlow.countBy (fun x -> func.Invoke(x)) stream

    static member Sum(stream : CloudFlow<int64>) = 
        CloudFlow.sum stream

    static member Sum(stream : CloudFlow<int>) = 
        CloudFlow.sum stream

    static member Sum(stream : CloudFlow<single>) = 
        CloudFlow.sum stream

    static member Sum(stream : CloudFlow<double>) = 
        CloudFlow.sum stream

    static member Sum(stream : CloudFlow<decimal>) = 
        CloudFlow.sum stream

    static member MaxBy<'T, 'Key when 'Key :> IComparable and 'Key : comparison>(flow : CloudFlow<'T>, projectionFunc : Func<'T, 'Key>) : Cloud<'T> =
        CloudFlow.maxBy (fun x -> projectionFunc.Invoke(x)) flow

    static member OfCloudFiles(paths : seq<string>, reader : Func<IO.Stream, seq<'T>>, sizeThresholdPerCore : Nullable<int64>) =
        CloudFlow.OfCloudFiles (paths, reader.Invoke, ?sizeThresholdPerCore = Option.ofNullable sizeThresholdPerCore)
