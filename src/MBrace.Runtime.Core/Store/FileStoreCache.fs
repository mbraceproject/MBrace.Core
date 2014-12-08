namespace Nessos.MBrace.Runtime.Store

open System
open System.IO
open Nessos.MBrace
open Nessos.MBrace.Store
open Nessos.MBrace.Runtime.Utils
open Nessos.MBrace.Runtime.Utils.String
open Nessos.MBrace.Runtime.Utils.Retry

/// Defines cache behavior.
[<Flags>]
type CacheBehavior = 
    /// Do not cache.
    | None = 0uy
    /// Cache only on write.
    | OnWrite = 1uy
    /// Cache only on read.
    | OnRead = 2uy
    /// Cache on read/write.
    | Default = 3uy

[<AutoSerializable(false) ; Sealed>]
type FileStoreCache private (cacheStore : ICloudFileStore, sourceStore : ICloudFileStore, 
                                    cacheContainer : string, ?cacheBehavior : CacheBehavior) =


    let getCachedFileName (path : string) = cacheStore.Combine [|cacheContainer ; Convert.StringToBase32 path|]

    let mutable cacheBehavior = defaultArg cacheBehavior CacheBehavior.Default

    member this.Container = cacheContainer

    member this.CacheStore = cacheStore
    member this.SourceStore = sourceStore

    member this.Behavior with get () = cacheBehavior and set b = cacheBehavior <- b

    interface ICloudFileStore with
        member x.BeginRead(path: string): Async<Stream> = async {
            if cacheBehavior.HasFlag CacheBehavior.OnRead then
                let cachedFileName = getCachedFileName path

                let rec attemptRead () = async {
                    let! sourceExists, cacheExists = Async.Parallel(sourceStore.FileExists path, cacheStore.FileExists cachedFileName)

                    if sourceExists then
                        if cacheExists then
                            try return! cacheStore.BeginRead cachedFileName
                            with e ->
                                // retry in case of concurrent cache writes
                                return! retryAsync (RetryPolicy.Retry(3, 0.5<sec>)) (attemptRead())
                        else
                            try
                                use! stream = sourceStore.BeginRead path
                                in do! cacheStore.OfStream(stream, cachedFileName)

                                return! cacheStore.BeginRead cachedFileName
                            with e ->
                                // retry in case of concurrent cache writes
                                return! retryAsync (RetryPolicy.Retry(3, 0.5<sec>)) (attemptRead())
//                    elif cacheExists then
//                        // this is the old behaviour; doesn't feel right
//                        return invalidOp <| sprintf "Incoherent cache : '%s' found in cache but not in the main store" path
                    else
                        return raise <| FileNotFoundException(path)
                }

                return! attemptRead()
            else
                return! sourceStore.BeginRead path
        }
        
        member x.BeginWrite(path: string): Async<Stream> =
            failwith "Not implemented yet"
        
        member x.Combine(paths: string []): string = 
            failwith "Not implemented yet"
        
        member x.CreateDirectory(directory: string): Async<unit> = 
            failwith "Not implemented yet"
        
        member x.CreateUniqueDirectoryPath(): string = 
            failwith "Not implemented yet"
        
        member x.DeleteDirectory(directory: string, recursiveDelete: bool): Async<unit> = 
            failwith "Not implemented yet"
        
        member x.DeleteFile(path: string): Async<unit> = 
            failwith "Not implemented yet"
        
        member x.DirectoryExists(directory: string): Async<bool> = 
            failwith "Not implemented yet"
        
        member x.EnumerateDirectories(directory: string): Async<string []> = 
            failwith "Not implemented yet"
        
        member x.EnumerateFiles(directory: string): Async<string []> = 
            failwith "Not implemented yet"
        
        member x.FileExists(path: string): Async<bool> = 
            failwith "Not implemented yet"
        
        member x.GetDirectoryName(path: string): string = 
            failwith "Not implemented yet"
        
        member x.GetFileName(path: string): string = 
            failwith "Not implemented yet"
        
        member x.GetFileSize(path: string): Async<int64> = 
            failwith "Not implemented yet"
        
        member x.GetFileStoreDescriptor(): ICloudFileStoreDescriptor = 
            failwith "Not implemented yet"
        
        member x.GetRootDirectory(): string = 
            failwith "Not implemented yet"
        
        member x.Id: string = 
            failwith "Not implemented yet"
        
        member x.Name: string = 
            failwith "Not implemented yet"
        
        member x.OfStream(source: Stream, target: string): Async<unit> = 
            failwith "Not implemented yet"
        
        member x.ToStream(sourceFile: string, target: Stream): Async<unit> = 
            failwith "Not implemented yet"
        
        member x.TryGetFullPath(path: string): string option = 
            failwith "Not implemented yet"
        