namespace Nessos.MBrace.Azure.Store

open System
open System.IO
open System.Security.AccessControl
open System.Runtime.Serialization

open Nessos.MBrace.Store
open Microsoft.WindowsAzure.Storage

/// Store implementation that uses a filesystem as backend.
[<Sealed;AutoSerializable(false)>]
type BlobStore (conn : string) =
    
    let acc = CloudStorageAccount.Parse(conn)

    let getContainer container = 
        let client = Clients.getBlobClient acc
        client.GetContainerReference container

    let getBlobRef path = async {
        let container, blob = toContainerFile path
        let container = getContainer container
        let! _ = Async.AwaitTask <| container.CreateIfNotExistsAsync()
        return container.GetBlockBlobReference(blob)
    }

    let validChars = set { 'a'..'z' } |> Set.add '-'

    interface ICloudFileStore with
        member x.UUID : string = acc.BlobStorageUri.ToString()

        member x.BeginRead(path: string): Async<Stream> = 
            async {
                let! blob = getBlobRef path
                return! Async.AwaitTask(blob.OpenReadAsync())
            }
        
        member x.BeginWrite(path: string): Async<Stream> = 
            async {
                let! blob = getBlobRef path
                let! stream = blob.OpenWriteAsync()
                return stream :> Stream
            } 
        
        member x.Combine(container: string, fileName: string): string = 
            Path.Combine(container, fileName)
        
        member x.ContainerExists(container: string): Async<bool> = 
            async {
                let container = getContainer container
                return! Async.AwaitTask <| container.ExistsAsync()
            }
        
        member x.CreateContainer(container: string): Async<unit> = 
            async {
                let container = getContainer container
                let! _ = Async.AwaitTask <| container.CreateIfNotExistsAsync()
                return ()
            }

        member x.CreateUniqueContainerName(): string = 
            Guid.NewGuid().ToString()
        
        member x.CreateUniqueFileName(container: string): string = 
            Path.Combine(container, Guid.NewGuid().ToString())
        
        member x.DeleteContainer(container: string): Async<unit> = 
            async {
                let container = getContainer container
                let! _ = Async.AwaitTask <| container.DeleteIfExistsAsync()
                return ()
            }
        
        member x.DeleteFile(path: string): Async<unit> = 
            async {
                let! blob = getBlobRef path
                let! _ =  Async.AwaitIAsyncResult <| blob.DeleteAsync()
                return ()
            }
        
        member x.EnumerateContainers(): Async<string []> = 
            async {
                let client = Clients.getBlobClient acc
                return client.ListContainers() 
                       |> Seq.map (fun c -> c.Name)
                       |> Seq.toArray
            }
        
        member x.EnumerateFiles(container: string): Async<string []> = 
            async {
                let cont = getContainer container
                return cont.ListBlobs()
                        |> Seq.map (fun blob ->  blob.Uri.Segments |> Seq.last)
                        |> Seq.map (fun y -> (x :> ICloudFileStore).Combine(container, y))
                        |> Seq.toArray
            }
        
        member x.FileExists(path: string): Async<bool> = 
            async {
                let container, file = toContainerFile path
                let container = getContainer container
                
                let! b1 = Async.AwaitTask(container.ExistsAsync())
                if b1 then
                    let blob = container.GetBlockBlobReference(file)
                    return! Async.AwaitTask(blob.ExistsAsync())
                else 
                    return false
            }
        
        
        member x.GetFileContainer(path: string): string = 
            Path.GetDirectoryName(path)
        
        member x.GetFileName(path: string): string = 
            Path.GetFileName(path)
        
        member x.GetFileSize(path: string): Async<int64> = 
            async {
                let! blob = getBlobRef path
                let! _ = Async.AwaitIAsyncResult <| blob.FetchAttributesAsync()
                return blob.Properties.Length
            }
        
        member x.IsValidPath(path: string): bool = 
            path.Length >= 3 && 
                path.Length <= 63 && 
                path |> String.forall validChars.Contains &&
                not <| path.Contains("--")
        
        member x.OfStream(source: Stream, target: string): Async<unit> = 
            async {
                let! blob = getBlobRef target
                let! _ = Async.AwaitIAsyncResult <| blob.UploadFromStreamAsync(source)
                return ()
            }
        
        member x.ToStream(sourceFile: string, target: Stream): Async<unit> = 
            async {
                let! blob = getBlobRef sourceFile
                let! _ = Async.AwaitIAsyncResult <| blob.DownloadToStreamAsync(target)
                return ()
            }
        
        member x.GetFactory() : ICloudFileStoreFactory = 
            let c = conn
            { new ICloudFileStoreFactory with
                  member x.Create() : ICloudFileStore = new BlobStore(c) :> _ }
