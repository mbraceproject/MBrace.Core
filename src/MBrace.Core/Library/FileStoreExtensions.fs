namespace MBrace.Store

[<AutoOpen>]
module FileStoreExtensions =

    type CloudFile with
    /// <summary>
    ///     Uploads a file from local disk to store.
    /// </summary>
    /// <param name="localFile">Path to file in local disk.</param>
    /// <param name="targetPath">Path to target file in cloud store.</param>
    /// <param name="overwrite">Enables overwriting of target file if it exists. Defaults to false.</param>
    static member Upload(localFile : string, targetPath : string, ?overwrite : bool) : Local<CloudFile> = local {
        let overwrite = defaultArg overwrite false
        let! config = Cloud.GetResource<CloudFileStoreConfiguration>()
        if not overwrite then
            let! exists = ofAsync <| config.FileStore.FileExists targetPath
            if exists then raise <| new IOException(sprintf "The file '%s' already exists." targetPath)

        use fs = File.OpenRead (Path.GetFullPath localFile)
        do! ofAsync <| config.FileStore.CopyOfStream(fs, targetPath)
        return new CloudFile(targetPath)
    }

    /// <summary>
    ///     Downloads a file from store to local disk.
    /// </summary>
    /// <param name="sourcePath">Source path to file in store.</param>
    /// <param name="targetPath">Path to target directory in local disk.</param>
    /// <param name="overwrite">Enables overwriting of target file if it exists. Defaults to false.</param>
    static member Download(sourcePath : string, targetPath : string, ?overwrite : bool) : Local<unit> = local {
        let overwrite = defaultArg overwrite false
        let targetPath = Path.GetFullPath targetPath
        let! config = Cloud.GetResource<CloudFileStoreConfiguration> ()
        if not overwrite && File.Exists targetPath then
            raise <| new IOException(sprintf "The file '%s' already exists." targetPath)

        use fs = File.OpenWrite targetPath
        do! ofAsync <| config.FileStore.CopyToStream(sourcePath, fs)
    }