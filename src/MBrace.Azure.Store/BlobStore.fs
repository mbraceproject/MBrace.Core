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

    interface ICloudFileStore with
        member this.Name = acc.BlobStorageUri.PrimaryUri.ToString()
        member this.Id : string = conn

        member this.GetRootDirectory () = String.Empty

        member this.GetUniqueDirectoryPath() : string = Guid.NewGuid().ToString()

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
                let container, file = toContainerFile path
                let container = getContainer container
                
                let! b1 = Async.AwaitTask(container.ExistsAsync())
                if b1 then
                    let blob = container.GetBlockBlobReference(file)
                    return! Async.AwaitTask(blob.ExistsAsync())
                else 
                    return false
            }

        member this.EnumerateFiles(container: string) : Async<string []> = 
            async {
                let cont = getContainer container
                return cont.ListBlobs()
                        |> Seq.map (fun blob ->  blob.Uri.Segments |> Seq.last)
                        |> Seq.map (fun y -> Path.Combine(container, y))
                        |> Seq.toArray
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
        
        member this.CreateDirectory(container: string) : Async<string> = 
            async {
                let container = getContainer container
                let! _ =  container.CreateIfNotExistsAsync()
                return container.Name
            }

        member this.DeleteDirectory(container: string, recursiveDelete : bool) : Async<unit> = 
            async {
                if not recursiveDelete then raise <| NotImplementedException("Non recursive delete")
                let container = getContainer container
                let! _ = container.DeleteIfExistsAsync()
                return ()
            }

        
        member this.EnumerateDirectories(directory) : Async<string []> = 
            async {
                let client = Clients.getBlobClient acc
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

        member this.GetFileStoreDescriptor() : ICloudFileStoreDescriptor = 
            let id = (this :> ICloudFileStore).Id
            let name = (this :> ICloudFileStore).Name
            { new ICloudFileStoreDescriptor with
                  member this.Id : string = id
                  member this.Name : string = name
                  member this.Recover() : ICloudFileStore = new BlobStore(this.Id) :> _
            }

//    let validChars = set { 'a'..'z' } |> Set.add '-'
//        member this.CreateUniqueFileName(container: string) : string = 
//            Path.Combine(container, Guid.NewGuid().ToString())
//
//        member this.GetFileContainer(path: string) : string = 
//            Path.GetDirectoryName(path)
//        
//        
//        member this.IsValidPath(path: string) : bool = 
//            path.Length >= 3 && 
//                path.Length <= 63 && 
//                path |> String.forall validChars.Contains &&
//                not <| path.Contains("--")
