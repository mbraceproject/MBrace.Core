namespace Nessos.MBrace.Azure.Store

open System
open System.IO
open System.Threading.Tasks
open Nessos.FsPickler
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table

[<AutoOpen>]
module internal Utils =

    let toContainerFile path =
        Path.GetDirectoryName(path), Path.GetFileName(path)

    let PayloadSizePerProperty = 64L * 1024L
    let NumberOfProperties = 15L
    let MaxPayloadSize = NumberOfProperties * PayloadSizePerProperty

    let partitionIn n (a : byte []) =
        let n = ((float a.Length) / (float n) |> ceil |> int)
        [| for i in 0 .. n - 1 ->
            let i, j = a.Length * i / n, a.Length * (i + 1) / n
            Array.sub a i (j - i) |]

    type Async with
        static member inline Cast<'T, 'U>(task : Async<'T>) = async { let! t = task in return box t :?> 'U }

    type AsyncBuilder with
        member inline __.Bind(f : Task<'T>, g : 'T -> Async<'S>) : Async<'S> = 
            async { let! r = Async.AwaitTask(f) in return! g r }

module internal Clients =
    open Microsoft.WindowsAzure.Storage

    let getTableClient (account : CloudStorageAccount) =
        let client = account.CreateCloudTableClient()
        client

    let getBlobClient (account : CloudStorageAccount) =
        let client = account.CreateCloudBlobClient()
            
        client.DefaultRequestOptions.ParallelOperationThreadCount <- System.Nullable(4 * System.Environment.ProcessorCount)
        client.DefaultRequestOptions.SingleBlobUploadThresholdInBytes <- System.Nullable(1L <<< 23) // 8MB, possible ranges: 1..64MB, default 32MB

        client


module internal Table =

    let PreconditionFailed (e : exn) =
        match e with
        | :? AggregateException as e ->
            let e = e.InnerException
            e :? StorageException && (e :?> StorageException).RequestInformation.HttpStatusCode = 412 
        | _ -> false

    let private exec<'U> acc table op : Async<obj> = 
        async {
            let t = Clients.getTableClient(acc).GetTableReference(table)
            let! _ = Async.AwaitTask(t.CreateIfNotExistsAsync())
            let! e = t.ExecuteAsync(op)
            return e.Result
        }

    let insert<'T when 'T :> ITableEntity> acc table (e : 'T) : Async<unit> = 
        TableOperation.Insert(e) |> exec acc table |> Async.Ignore
    
    let read<'T when 'T :> ITableEntity> acc table pk rk : Async<'T> = 
        async { 
            let t = Clients.getTableClient(acc).GetTableReference(table)
            let! e = t.ExecuteAsync(TableOperation.Retrieve<'T>(pk, rk))
            return e.Result :?> 'T
        }
    
    let merge<'T when 'T :> ITableEntity> acc table (e : 'T) : Async<'T> = 
        TableOperation.Merge(e) |> exec acc table |> Async.Cast 

    let delete<'T when 'T :> ITableEntity> acc table (e : 'T) : Async<unit> =
        TableOperation.Delete(e) |> exec acc table |> Async.Ignore


/// A lightweight object for low latency communication with the azure storage.
/// Lightweight : payload size up to 15 * 64K = 960K.
/// See 'http://www.windowsazure.com/en-us/develop/net/how-to-guides/table-services/'
/// WARNING : See the above link for any restrictions such as having a parameterless ctor,
/// and public properties.
[<AllowNullLiteral>]
type FatEntity (pk, rk, binary) =
    inherit TableEntity(pk, rk)

    let check (a : byte [] []) i = 
        let i = i - 1
        if a = null then null elif i >= a.Length then Array.empty else a.[i]
    let binaries = 
        if binary <> null 
        then partitionIn PayloadSizePerProperty binary
        else null

    /// Max size 64KB
    member val Item01 = check binaries 1  with get, set
    member val Item02 = check binaries 2  with get, set
    member val Item03 = check binaries 3  with get, set
    member val Item04 = check binaries 4  with get, set
    member val Item05 = check binaries 5  with get, set
    member val Item06 = check binaries 6  with get, set
    member val Item07 = check binaries 7  with get, set
    member val Item08 = check binaries 8  with get, set
    member val Item09 = check binaries 9  with get, set
    member val Item10 = check binaries 10 with get, set
    member val Item11 = check binaries 11 with get, set
    member val Item12 = check binaries 12 with get, set
    member val Item13 = check binaries 13 with get, set
    member val Item14 = check binaries 14 with get, set
    member val Item15 = check binaries 15 with get, set

    member this.GetPayload () = 
        [| this.Item01; this.Item02; this.Item03; this.Item04; this.Item05; this.Item06; this.Item07; this.Item08; this.Item09; 
           this.Item10; this.Item11; this.Item12; this.Item13; this.Item14; this.Item15; |]
        |> Array.map (fun a -> if a = null then Array.empty else a)
        |> Array.concat
        
    new () = FatEntity (null, null, null)



(*
    //http://msdn.microsoft.com/en-us/library/azure/dd179338.aspx

    let generate arity =
        let sb = new System.Text.StringBuilder()
        let generics = {1..arity} |> Seq.map (sprintf "'T%02d") |> String.concat ", "
        let args = {1..arity} |> Seq.map(fun i -> sprintf "item%02d : 'T%02d" i i) |> String.concat ", "
        let param = {1..arity} |> Seq.map (sprintf "Unchecked.defaultof<'T%02d>") |> String.concat ", "
        Printf.bprintf sb "type TupleEntity<%s> (pk : string, rk : string, %s) =\n" generics args
        Printf.bprintf sb "    inherit TableEntity(pk, rk)\n"
        Printf.bprintf sb "    member val Item%02d = item%02d with get, set\n" |> fun f -> Seq.iter (fun i -> f i i) {1..arity}
        Printf.bprintf sb "    new () = TupleEntity<%s>(null, null,%s)\n" generics param
        sb.ToString()

    for i = 1 to 16 do
        printfn "%s" <| generate i
*)
(*
module DynamicEntity =

    let create<'T> pk rk (value : 'T) serialize =
        let e = new DynamicTableEntity(pk, rk)
        let prop =
            match box value with
            | :? bool           as value -> Some <| EntityProperty.GeneratePropertyForBool(new Nullable<_>(value))
            | :? DateTime       as value -> Some <| EntityProperty.GeneratePropertyForDateTimeOffset(new Nullable<_>(new DateTimeOffset(value)))
            | :? DateTimeOffset as value -> Some <| EntityProperty.GeneratePropertyForDateTimeOffset(new Nullable<_>(value))
            | :? double         as value -> Some <| EntityProperty.GeneratePropertyForDouble(new Nullable<_>(value))
            | :? Guid           as value -> Some <| EntityProperty.GeneratePropertyForGuid(new Nullable<_>(value))
            | :? int            as value -> Some <| EntityProperty.GeneratePropertyForInt(new Nullable<_>(value))
            | :? int64          as value -> Some <| EntityProperty.GeneratePropertyForLong(new Nullable<_>(value))
            | :? string         as value -> Some <| EntityProperty.GeneratePropertyForString(value)
            | _ -> None
        match prop with
        | Some p ->
            e.Properties.Add("Value", p)
        | None ->
            let binary = serialize value
            
            let binaries = 
                if binary = null then null
                else partitionIn PayloadSizePerProperty binary
            
            binaries 
            |> Array.iteri (fun i b -> e.Properties.Add(sprintf "Item%0d" (i + 1), EntityProperty.GeneratePropertyForByteArray(b)))
        e
*)