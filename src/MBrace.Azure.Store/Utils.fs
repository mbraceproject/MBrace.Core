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

    type AsyncBuilder with
        member inline __.Bind(f : Task<'T>, g : 'T -> Async<'S>) : Async<'S> = 
            async { let! r = Async.AwaitTask(f) in return! g r }

module internal Clients =
    open Microsoft.WindowsAzure.Storage

    let getBlobClient (account : CloudStorageAccount) =
        let client = account.CreateCloudBlobClient()
            
        client.DefaultRequestOptions.ParallelOperationThreadCount <- System.Nullable(4 * System.Environment.ProcessorCount)
        client.DefaultRequestOptions.SingleBlobUploadThresholdInBytes <- System.Nullable(1L <<< 23) // 8MB, possible ranges: 1..64MB, default 32MB

        client