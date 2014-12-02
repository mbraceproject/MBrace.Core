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

    let serializer = FsPickler.CreateBinary()

    let serialize (value : 'T) = serializer.Pickle(value)

    let deserialize<'T> binary = serializer.UnPickle<'T>(binary)

    let partitionIn n (a : byte []) =
        let n = ((float a.Length) / (float n) |> ceil |> int)
        [| for i in 0 .. n - 1 ->
            let i, j = a.Length * i / n, a.Length * (i + 1) / n
            Array.sub a i (j - i) |]

    let isFatEntity (item : 'T) =
        serializer.ComputeSize(item) <= MaxPayloadSize

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
type FatEntity (entityId, binary) =
    inherit TableEntity(entityId, String.Empty)

    let check (a : byte [] []) i = if a = null then null elif i >= a.Length then Array.empty else a.[i]
    let binaries = 
        if binary <> null 
        then partitionIn PayloadSizePerProperty binary
        else null

    /// Max size 64KB
    member val Part00 = check binaries 0  with get, set
    member val Part01 = check binaries 1  with get, set
    member val Part02 = check binaries 2  with get, set
    member val Part03 = check binaries 3  with get, set
    member val Part04 = check binaries 4  with get, set
    member val Part05 = check binaries 5  with get, set
    member val Part06 = check binaries 6  with get, set
    member val Part07 = check binaries 7  with get, set
    member val Part08 = check binaries 8  with get, set
    member val Part09 = check binaries 9  with get, set
    member val Part10 = check binaries 10 with get, set
    member val Part11 = check binaries 11 with get, set
    member val Part12 = check binaries 12 with get, set
    member val Part13 = check binaries 13 with get, set
    member val Part14 = check binaries 14 with get, set

    member this.GetPayload () = 
        [|this.Part00 ; this.Part01; this.Part02; this.Part03
          this.Part04 ; this.Part05; this.Part06; this.Part07
          this.Part08 ; this.Part09; this.Part10; this.Part11
          this.Part12 ; this.Part13; this.Part14 |]
        |> Array.map (fun a -> if a = null then Array.empty else a)
        |> Array.concat
        
    new () = FatEntity (null, null)