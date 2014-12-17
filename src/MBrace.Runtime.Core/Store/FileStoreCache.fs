namespace Nessos.MBrace.Runtime.Store

open System
open System.IO
open System.Collections.Generic
open System.Collections.Concurrent
open System.Runtime.Serialization

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

// Combines two write-only streams into one.
type private StreamCombiner(s1 : Stream, s2 : Stream) =
    inherit Stream()

    override __.Length = s1.Length
    override __.ReadTimeout = min s1.ReadTimeout s2.ReadTimeout
    override __.Position 
        with get () = s1.Position
        and set p = s1.Position <- p ; s2.Position <- p

    override __.CanRead = false
    override __.Read(_,_,_) = raise <| new NotSupportedException()

    override __.CanSeek = s1.CanSeek && s2.CanSeek
    override __.Seek(i,o) = 
        let i1 = s1.Seek(i,o) 
        let i2 = s2.Seek(i,o)
        i1

    override __.CanTimeout = s1.CanTimeout || s2.CanTimeout
    override __.WriteTimeout = min s1.WriteTimeout s2.WriteTimeout
    override __.CanWrite = true
    override __.WriteByte(b : byte) = s1.WriteByte b ; s2.WriteByte b
    override __.Write(buf : byte[], offset, count) = s1.Write(buf, offset, count) ; s2.Write(buf, offset, count)

    override __.SetLength l = raise <| new NotSupportedException()
    override __.Flush () = s1.Flush() ; s2.Flush()

    interface IDisposable with
        member __.Dispose () = s1.Dispose() ; s2.Dispose()

/// File store caching facility
[<Sealed; DataContract>]
type FileStoreCache private (cacheContext : string, localCacheStore : ICloudFileStore, 
                                cacheBehavior : CacheBehavior, sourceStore : ICloudFileStore) =

    static let cacheEvent = new Event<ICloudFileStore * string> ()
    
    [<Literal>]
    static let defaultContext = "DefaultCacheContext"
    static let localCaches = new ConcurrentDictionary<string, ICloudFileStore * CacheBehavior> ()
    static let localCacheContainers = new ConcurrentDictionary<string, string> ()
    static let getCachedContainerName (ctx : string) (localCacheStore : ICloudFileStore) (store : ICloudFileStore) =
        let key = sprintf "%s::%O::%s" ctx (store.GetType()) store.Id
        localCacheContainers.GetOrAdd(key, fun _ -> localCacheStore.CreateUniqueDirectoryPath())

    [<DataMember(Name = "SourceStore")>]
    let sourceStore = sourceStore

    [<DataMember(Name = "CacheContext")>]
    let cacheContext = cacheContext

    [<IgnoreDataMember>]
    let mutable localCacheContainer = getCachedContainerName cacheContext localCacheStore sourceStore

    [<IgnoreDataMember>]
    let mutable cacheBehavior = cacheBehavior

    [<IgnoreDataMember>]
    let mutable localCacheStore = localCacheStore

    [<OnDeserialized>]
    let onDeserialized (_ : StreamingContext) =
        let ok, (lcs, cb) = localCaches.TryGetValue cacheContext
        if ok then
            localCacheStore <- lcs
            cacheBehavior <- cb
            localCacheContainer <- getCachedContainerName cacheContext localCacheStore sourceStore
        else
            invalidOp <| sprintf "No local cache store has been installed under '%s'." cacheContext

    let getCachedFileName (path : string) = localCacheStore.Combine [|localCacheContainer ; Convert.StringToBase32 path|]

    static member OnCache = cacheEvent.Publish

    member this.Container = localCacheContainer
    member this.CacheStore = localCacheStore
    member this.SourceStore = sourceStore

    /// <summary>
    ///     Installs a local file store cache under given context.
    /// </summary>
    /// <param name="localCacheStore">Local cache store.</param>
    /// <param name="cacheContext">Cache context identifier. Must be identical in all caching nodes.</param>
    /// <param name="cacheBehavior">Caching behavior. Defaults to all.</param>
    static member RegisterLocalCacheStore(localCacheStore : ICloudFileStore, ?cacheContext : string, ?cacheBehavior) : unit =
        let cacheContext = defaultArg cacheContext defaultContext
        let cacheBehavior = defaultArg cacheBehavior CacheBehavior.Default
        if not <| localCaches.TryAdd(cacheContext, (localCacheStore, cacheBehavior)) then
            invalidOp <| sprintf "A a cache store has already been declared under '%s'." cacheContext

    /// <summary>
    ///     Defines a locally specified file system cache store.
    /// </summary>
    /// <param name="cacheContext">Cache context identifier. Must be identical in all caching nodes.</param>
    /// <param name="cacheBehavior">Caching behavior. Defaults to all.</param>
    static member RegisterLocalFileSystemCache(?cacheContext : string, ?cacheBehavior) : unit =
        let localPath = Path.Combine(Path.GetTempPath(), sprintf "mbrace-cache-%d" <| System.Diagnostics.Process.GetCurrentProcess().Id)
        let localFileSystemStore = FileSystemStore.Create(localPath, create = true, cleanup = true)
        FileStoreCache.RegisterLocalCacheStore(localFileSystemStore, ?cacheContext = cacheContext, ?cacheBehavior = cacheBehavior)

    /// <summary>
    ///     Creates a file store cache instance.
    /// </summary>
    /// <param name="target">Target file store to be cached.</param>
    /// <param name="cacheContext">Caching context identifier. Must be identical in all caching nodes.</param>
    static member CreateCachedStore(target : ICloudFileStore, ?cacheContext : string) : FileStoreCache =
        let cacheContext = defaultArg cacheContext defaultContext
        let ok, (localCacheStore, cacheBehavior) = localCaches.TryGetValue cacheContext
        if ok then
            new FileStoreCache(cacheContext, localCacheStore, cacheBehavior, target)
        else
            invalidOp <| sprintf "No local cache store has been installed under '%s'." cacheContext
            

    interface ICloudFileStore with
        member x.BeginRead(path: string): Async<Stream> = async {
            if cacheBehavior.HasFlag CacheBehavior.OnRead then
                let cachedFileName = getCachedFileName path

                let rec attemptRead retries = async {
                    let! sourceExists, cacheExists = Async.Parallel(sourceStore.FileExists path, localCacheStore.FileExists cachedFileName)

                    if sourceExists then
                        if cacheExists then
                            try return! localCacheStore.BeginRead cachedFileName
                            with e when retries > 0 ->
                                // retry in case of concurrent cache writes
                                return! attemptRead (retries - 1)
                        else
                            try
                                use! stream = sourceStore.BeginRead path
                                in do! localCacheStore.OfStream(stream, cachedFileName)

                                let _ = cacheEvent.TriggerAsTask(sourceStore, path)

                                return! localCacheStore.BeginRead cachedFileName
                            with e when retries > 0 ->
                                // retry in case of concurrent cache writes
                                return! attemptRead (retries - 1)
                    else
                        return raise <| FileNotFoundException(path)
                }

                return! attemptRead 3
            else
                return! sourceStore.BeginRead path
        }
        
        member x.BeginWrite(path: string): Async<Stream> = async {
            if cacheBehavior.HasFlag CacheBehavior.OnWrite then
                let cachedFile = getCachedFileName path
                // check if file exists in local cache first.
                let! cachedFileExists = Async.StartChild(localCacheStore.FileExists cachedFile)
                let! sourceStream = sourceStore.BeginWrite path
                let! cachedFileExists = cachedFileExists
                if cachedFileExists then do! retryAsync (RetryPolicy.Retry(3, 0.5<sec>)) (localCacheStore.DeleteFile cachedFile)
                let! cacheStream = localCacheStore.BeginWrite(getCachedFileName path)
                let _ = cacheEvent.TriggerAsTask(sourceStore, path)
                return new StreamCombiner(sourceStream, cacheStream) :> Stream
            else
                return! sourceStore.BeginWrite path
        }
        
        member x.Combine(paths: string []): string = sourceStore.Combine paths
        
        member x.CreateDirectory(directory: string): Async<unit> = sourceStore.CreateDirectory directory
        
        member x.CreateUniqueDirectoryPath(): string = sourceStore.CreateUniqueDirectoryPath()
        
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