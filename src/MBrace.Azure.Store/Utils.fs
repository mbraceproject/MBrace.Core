namespace Nessos.MBrace.Azure.Store

[<AutoOpen>]
module internal Helpers =
    open System.IO

    let toContainerFile path =
        Path.GetDirectoryName(path), Path.GetFileName(path)

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