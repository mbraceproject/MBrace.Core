namespace Nessos.MBrace.Azure.Store

open System
open System.IO
open System.Security.AccessControl
open System.Runtime.Serialization

open Nessos.MBrace.Store
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Blob

///  Store implementation that uses a Azure Blob Storage as backend.
[<Sealed; DataContract>]
type BlobStore (connectionString : string) =

    [<DataMember(Name = "ConnectionString")>]
    let connectionString = connectionString

    [<IgnoreDataMember>]
    let mutable acc = CloudStorageAccount.Parse(connectionString)

    [<OnDeserialized>]
    let onDeserialized (_ : StreamingContext) =
        acc <- CloudStorageAccount.Parse(connectionString)

    let getBlobRef = getBlobRef acc
    let getContainer = getContainer acc

    interface ICloudFileStore with
        member this.Name = "MBrace.Azure.Store.BlobStore"
        member this.Id : string = acc.BlobStorageUri.PrimaryUri.ToString()

        member this.GetRootDirectory () = String.Empty

        member this.CreateUniqueDirectoryPath() : string = Guid.NewGuid().ToString()

        member this.TryGetFullPath(path : string) = Some path

        member this.GetDirectoryName(path : string) = Path.GetDirectoryName(path)

        member this.GetFileName(path : string) = Path.GetFileName(path)

        member this.Combine(paths : string []) : string = 
            Path.Combine(paths)

        member this.GetFileSize(path: string) : Async<int64> = 
            async {
                let! blob = getBlobRef path
                let! _ = Async.AwaitIAsyncResult <| blob.FetchAttributesAsync()
                return blob.Properties.Length
            }
        member this.FileExists(path: string) : Async<bool> = 
            async {
                let container, file = splitPath path
                let container = getContainer container
                
                let! b1 = Async.AwaitTask(container.ExistsAsync())
                if b1 then
                    let blob = container.GetBlockBlobReference(file)
                    return! Async.AwaitTask(blob.ExistsAsync())
                else 
                    return false
            }

        member this.EnumerateFiles(container : string) : Async<string []> = 
            async {
                let containerRef = getContainer container
                let blobs = new ResizeArray<string>()
                let rec aux (token : BlobContinuationToken) = async {
                    let! (result : BlobResultSegment) = containerRef.ListBlobsSegmentedAsync(token)
                    for blob in result.Results do
                        let p = blob.Uri.Segments |> Seq.last
                        blobs.Add(Path.Combine(container, p))
                    if result.ContinuationToken = null then return ()
                    else return! aux result.ContinuationToken
                }
                do! aux null
                return blobs.ToArray()
            }
        
        member this.DeleteFile(path: string) : Async<unit> = 
            async {
                let! blob = getBlobRef path
                let! _ =  Async.AwaitIAsyncResult <| blob.DeleteAsync()
                return ()
            }

        member this.DirectoryExists(container: string) : Async<bool> = 
            async {
                let container = getContainer container
                return! Async.AwaitTask <| container.ExistsAsync()
            }
        
        member this.CreateDirectory(container: string) : Async<unit> = 
            async {
                let container = getContainer container
                let! _ =  container.CreateIfNotExistsAsync()
                return ()
            }

        member this.DeleteDirectory(container: string, recursiveDelete : bool) : Async<unit> = 
            async {
                ignore recursiveDelete
                let container = getContainer container
                let! _ = container.DeleteIfExistsAsync()
                return ()
            }
        
        member this.EnumerateDirectories(directory) : Async<string []> = 
            async {
                let client = getBlobClient acc
                return client.ListContainers(directory) 
                       |> Seq.map (fun c -> c.Name)
                       |> Seq.toArray
            }

        member this.BeginWrite(path: string) : Async<Stream> = 
            async {
                let! blob = getBlobRef path
                let! stream = blob.OpenWriteAsync()
                return stream :> Stream
            } 
        
        member this.BeginRead(path: string) : Async<Stream> = 
            async {
                let! blob = getBlobRef path
                return! Async.AwaitTask(blob.OpenReadAsync())
            }

        member this.OfStream(source: Stream, target: string) : Async<unit> = 
            async {
                let! blob = getBlobRef target
                let! _ = Async.AwaitIAsyncResult <| blob.UploadFromStreamAsync(source)
                return ()
            }
        
        member this.ToStream(sourceFile: string, target: Stream) : Async<unit> = 
            async {
                let! blob = getBlobRef sourceFile
                let! _ = Async.AwaitIAsyncResult <| blob.DownloadToStreamAsync(target)
                return ()
            }