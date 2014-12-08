namespace Nessos.MBrace.Azure.Store

open System
open System.IO
open System.Threading.Tasks
open Nessos.FsPickler
open Microsoft.WindowsAzure.Storage

[<AutoOpen>]
module internal Utils =

    type AsyncBuilder with
        member inline __.Bind(f : Task<'T>, g : 'T -> Async<'S>) : Async<'S> = 
            async { let! r = Async.AwaitTask(f) in return! g r }

        member inline __.Bind(f : Task, g : unit -> Async<'S>) : Async<'S> = 
            async { let! r = Async.AwaitTask(f.ContinueWith ignore) in return! g r }

    let splitPath path = 
        Path.GetDirectoryName(path), Path.GetFileName(path)

    let getBlobClient (account : CloudStorageAccount) =
        let client = account.CreateCloudBlobClient()
            
        client.DefaultRequestOptions.ParallelOperationThreadCount <- System.Nullable(4 * System.Environment.ProcessorCount)
        client.DefaultRequestOptions.SingleBlobUploadThresholdInBytes <- System.Nullable(1L <<< 23) // 8MB, possible ranges: 1..64MB, default 32MB

        client

    let getContainer acc container = 
        let client = getBlobClient acc
        client.GetContainerReference container

    let getBlobRef acc path = async {
        let container, blob = splitPath path
        let container = getContainer acc container
        let! _ = container.CreateIfNotExistsAsync()
        return container.GetBlockBlobReference(blob)
    }