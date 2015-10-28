namespace MBrace.Runtime.Store

open System
open System.IO
open System.Collections.Generic
open System.Collections.Concurrent
open System.Runtime.Serialization
open System.Security.Cryptography
open System.Text
open System.Threading

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime.Utils
open MBrace.Runtime.Utils.String
open MBrace.Runtime.Utils.Retry

/// File store caching facility
[<Sealed; AutoSerializable(false)>]
type FileStoreCache private (sourceStore : ICloudFileStore, localCacheStore : ICloudFileStore, localCacheContainer : string) =
    static let hash = new ThreadLocal<_>(fun () -> MD5.Create() :> HashAlgorithm)
    let getCachedFileName (path : string) (tag : ETag) =
        let digestedId =
            path + ":" + tag
            |> Encoding.Default.GetBytes
            |> hash.Value.ComputeHash
            |> Convert.BytesToBase32

        let fileName = sourceStore.GetFileName path
        localCacheStore.Combine [|localCacheContainer ; fileName + "-" + digestedId |]

    let cacheEvent = new Event<string> ()

    /// Cache container in local store.
    member this.CacheContainer = localCacheContainer
    /// Local caching store in current machine.
    member this.CacheStore = localCacheStore
    /// Source store to be cached.
    member this.SourceStore = sourceStore

    /// <summary>
    ///     Wraps a file store instance in an instance that caches files to a cloud file store.
    /// </summary>
    /// <param name="target">Target file store to cache files from.</param>
    /// <param name="localCacheStore">Local file store to cache files to.</param>
    /// <param name="localCacheContainer">Directory in local store to contain cached files. Defaults to self-assigned.</param>
    static member Create(target : ICloudFileStore, localCacheStore : ICloudFileStore, ?localCacheContainer : string) : FileStoreCache =
        let localCacheContainer =
            match localCacheContainer with 
            | None -> localCacheStore.GetRandomDirectoryName()
            | Some c -> c

        do localCacheStore.CreateDirectory(localCacheContainer) |> Async.RunSync

        new FileStoreCache(target, localCacheStore, localCacheContainer)

    // TODO : cache range

    /// <summary>
    ///     Begins reading a cached copy of the stored file.
    ///     If file does not exist in local cache it will be downloaded
    ///     before reading begins.
    /// </summary>
    /// <param name="path">Path to remote file.</param>
    member __.BeginReadCached(path: string) : Async<Stream> = async {
        let rec attemptRead retries = async {
            let! etag = sourceStore.TryGetETag path

            match etag with
            | Some tag ->
                let cachedFileName = getCachedFileName path tag
                let! cacheExists = localCacheStore.FileExists cachedFileName
                if cacheExists then
                    try return! localCacheStore.BeginRead cachedFileName
                    with _ when retries > 0 ->
                        // retry in case of concurrent cache writes
                        do! Async.Sleep 100
                        return! attemptRead (retries - 1)
                else
                    try
                        let! streamOpt = sourceStore.ReadETag(path, tag)
                        match streamOpt with
                        | None -> 
                            do! Async.Sleep 1000
                            return! attemptRead retries

                        | Some stream ->
                            use stream = stream
                            let cachedFileName = getCachedFileName path tag
                            let! _ = localCacheStore.UploadFromStream(cachedFileName, stream)
                            let _ = cacheEvent.TriggerAsTask path
                            return! localCacheStore.BeginRead cachedFileName

                    with _ when retries > 0 ->
                        // retry in case of concurrent cache writes
                        do! Async.Sleep 100
                        return! attemptRead (retries - 1)

            | None -> return raise <| FileNotFoundException(path)
        }

        return! attemptRead 3
    }