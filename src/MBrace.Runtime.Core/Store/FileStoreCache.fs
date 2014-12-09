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

    override __.SetLength l = s1.SetLength l ; s2.SetLength l
    override __.Flush () = s1.Flush() ; s2.Flush()


[<AutoSerializable(false) ; Sealed>]
type FileStoreCache private (cacheStore : ICloudFileStore, sourceStore : ICloudFileStore, 
                                cacheContainer : string, cacheBehavior : CacheBehavior, 
                                descr : ICloudFileStore -> ICloudFileStoreDescriptor) =

    let getCachedFileName (path : string) = cacheStore.Combine [|cacheContainer ; Convert.StringToBase32 path|]

    let mutable cacheBehavior = cacheBehavior

    member this.Container = cacheContainer

    member this.CacheStore = cacheStore
    member this.SourceStore = sourceStore

    member this.Behavior with get () = cacheBehavior and set b = cacheBehavior <- b

    interface ICloudFileStore with
        member x.BeginRead(path: string): Async<Stream> = async {
            if cacheBehavior.HasFlag CacheBehavior.OnRead then
                let cachedFileName = getCachedFileName path

                let rec attemptRead retries = async {
                    let! sourceExists, cacheExists = Async.Parallel(sourceStore.FileExists path, cacheStore.FileExists cachedFileName)

                    if sourceExists then
                        if cacheExists then
                            try return! cacheStore.BeginRead cachedFileName
                            with e when retries > 0 ->
                                // retry in case of concurrent cache writes
                                return! attemptRead (retries - 1)
                        else
                            try
                                use! stream = sourceStore.BeginRead path
                                in do! cacheStore.OfStream(stream, cachedFileName)

                                return! cacheStore.BeginRead cachedFileName
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
                let! cachedFileExists = Async.StartChild(cacheStore.FileExists cachedFile)
                let! sourceStream = sourceStore.BeginWrite path
                let! cachedFileExists = cachedFileExists
                if cachedFileExists then do! retryAsync (RetryPolicy.Retry(3, 0.5<sec>)) (cacheStore.DeleteFile cachedFile)
                let! cacheStream = cacheStore.BeginWrite(getCachedFileName path)
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
        
        member x.GetFileStoreDescriptor(): ICloudFileStoreDescriptor = descr sourceStore
        
        member x.GetRootDirectory(): string = sourceStore.GetRootDirectory()
        
        member x.Id: string = sourceStore.Id
        
        member x.Name: string = sourceStore.Name
        
        member x.OfStream(source: Stream, target: string): Async<unit> = sourceStore.OfStream(source, target)
        
        member x.ToStream(sourceFile: string, target: Stream): Async<unit> = sourceStore.ToStream(sourceFile, target)
        
        member x.TryGetFullPath(path: string): string option = sourceStore.TryGetFullPath(path)