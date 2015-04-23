namespace MBrace.Runtime.Store

open System
open System.IO
open System.Collections.Generic
open System.Collections.Concurrent
open System.Runtime.Serialization

open MBrace
open MBrace.Store
open MBrace.Store.Internals
open MBrace.Runtime.Utils
open MBrace.Runtime.Utils.String
open MBrace.Runtime.Utils.Retry

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

/// File store caching facility
[<Sealed; AutoSerializable(false)>]
type FileStoreCache private (sourceStore : ICloudFileStore, localCacheStore : ICloudFileStore, 
                                cacheBehavior : CacheBehavior, localCacheContainer : string) =

    let getCachedFileName (path : string) = localCacheStore.Combine [|localCacheContainer ; Convert.StringToBase32 path|]
    let cacheEvent = new Event<string> ()

    member this.Container = localCacheContainer
    member this.CacheStore = localCacheStore
    member this.SourceStore = sourceStore

    /// <summary>
    ///     Wraps a file store instance in an instance that caches files to a cloud file store.
    /// </summary>
    /// <param name="target">Target file store to cache files from.</param>
    /// <param name="localCacheStore">Local file store to cache files to.</param>
    /// <param name="cacheBehavior">Caching behavior. Defaults to Read/Write caching.</param>
    /// <param name="localCacheContainer">Directory in local store to contain cached files. Defaults to self-assigned.</param>
    static member Create(target : ICloudFileStore, localCacheStore : ICloudFileStore, ?cacheBehavior, ?localCacheContainer) : FileStoreCache =
        let cacheBehavior = defaultArg cacheBehavior CacheBehavior.Default
        let localCacheContainer =
            match localCacheContainer with 
            | None -> localCacheStore.GetRandomDirectoryName()
            | Some c -> c

        do localCacheStore.CreateDirectory(localCacheContainer) |> Async.RunSynchronously

        new FileStoreCache(target, localCacheStore, cacheBehavior, localCacheContainer)

    interface ICloudFileStore with
        member x.BeginRead(path: string): Async<Stream> = async {
            if cacheBehavior.HasFlag CacheBehavior.OnRead then
                let cachedFileName = getCachedFileName path

                let rec attemptRead retries = async {
                    let! sourceExists, cacheExists = Async.Parallel(sourceStore.FileExists path, localCacheStore.FileExists cachedFileName)

                    if sourceExists then
                        if cacheExists then
                            try return! localCacheStore.BeginRead cachedFileName
                            with _ when retries > 0 ->
                                // retry in case of concurrent cache writes
                                return! attemptRead (retries - 1)
                        else
                            try
                                use! stream = sourceStore.BeginRead path
                                in do! localCacheStore.OfStream(stream, cachedFileName)

                                let _ = cacheEvent.TriggerAsTask path

                                return! localCacheStore.BeginRead cachedFileName
                            with _ when retries > 0 ->
                                // retry in case of concurrent cache writes
                                return! attemptRead (retries - 1)
                    else
                        return raise <| FileNotFoundException(path)
                }

                return! attemptRead 3
            else
                return! sourceStore.BeginRead path
        }
        
        member x.Write(path: string, writer : Stream -> Async<'R>): Async<'R> = async {
            if cacheBehavior.HasFlag CacheBehavior.OnWrite then
                let cachedFile = getCachedFileName path
                // check if files exist
                let! cachedFileExists, sourceFileExists = Async.Parallel(localCacheStore.FileExists cachedFile, sourceStore.FileExists path)
                // fail if source path exists in store, delete local copy if already cached
                if sourceFileExists then raise <| IOException(sprintf "The file '%s' already exists in store." path)
                if cachedFileExists then do! retryAsync (RetryPolicy.Retry(3, 0.5<sec>)) (localCacheStore.DeleteFile cachedFile)
                // write contents to local cache store
                let! r = localCacheStore.Write(cachedFile, writer)
                // use stream copying API writing to remote store
                use! stream = localCacheStore.BeginRead cachedFile
                in do! sourceStore.OfStream(stream, path)
                // trigger cache event and return result
                let _ = cacheEvent.TriggerAsTask path
                return r
            else
                return! sourceStore.Write(path, writer)
        }
        
        member x.Combine(paths: string []): string = sourceStore.Combine paths
        
        member x.CreateDirectory(directory: string): Async<unit> = sourceStore.CreateDirectory directory
        
        member x.GetRandomDirectoryName(): string = sourceStore.GetRandomDirectoryName()
        
        member x.DeleteDirectory(directory: string, recursiveDelete: bool): Async<unit> = sourceStore.DeleteDirectory(directory, recursiveDelete)
        
        member x.DeleteFile(path: string): Async<unit> = sourceStore.DeleteFile(path)
        
        member x.DirectoryExists(directory: string): Async<bool> = sourceStore.DirectoryExists(directory)
        
        member x.EnumerateDirectories(directory: string): Async<string []> = sourceStore.EnumerateDirectories(directory)
        
        member x.EnumerateFiles(directory: string): Async<string []> = sourceStore.EnumerateFiles(directory)
        
        member x.FileExists(path: string): Async<bool> = sourceStore.FileExists(path)
        
        member x.GetDirectoryName(path: string): string = sourceStore.GetDirectoryName(path)
        
        member x.GetFileName(path: string): string = sourceStore.GetFileName(path)
        
        member x.GetFileSize(path: string): Async<int64> = sourceStore.GetFileSize(path)
        
        member x.GetRootDirectory(): string = sourceStore.GetRootDirectory()
        
        member x.Id: string = sourceStore.Id
        
        member x.Name: string = sourceStore.Name
        
        member x.OfStream(source: Stream, target: string): Async<unit> = sourceStore.OfStream(source, target)
        
        member x.ToStream(sourceFile: string, target: Stream): Async<unit> = sourceStore.ToStream(sourceFile, target)
        
        member x.TryGetFullPath(path: string): string option = sourceStore.TryGetFullPath(path)