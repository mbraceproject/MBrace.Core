namespace MBrace.Runtime

open System
open System.IO
open System.Text.RegularExpressions
open System.Runtime.Serialization

open Nessos.Vagabond
open Nessos.Vagabond.AssemblyProtocols

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Library

open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.InMemoryRuntime
open MBrace.Runtime.Vagabond

#nowarn "1571"

[<AutoOpen>]
module private Common =

    /// Gets a unique blob filename for provided assembly
    let filename (id : AssemblyId) = Vagabond.GetFileName id

    let getStoreAssemblyPath k id = k <| filename id + id.Extension
    let getStoreSymbolsPath k id = k <| filename id + ".pdb"
    let getStoreMetadataPath k id = k <| filename id + ".vgb"
    let getStoreDataPath k id (dd : DataDependencyInfo) = k <| sprintf "%s-%d-%d.dat" (filename id) dd.Id dd.Generation

/// Assembly to file store uploader implementation
[<AutoSerializable(false)>]
type private StoreAssemblyUploader(config : CloudFileStoreConfiguration, imem : InMemoryRuntime, assemblyContainer : string, logger : ISystemLogger) =
    let sizeOfFile (path:string) = FileInfo(path).Length |> getHumanReadableByteSize
    let append (fileName : string) = config.FileStore.Combine(assemblyContainer, fileName)

    let tryGetCurrentMetadata (id : AssemblyId) = local {
        try 
            let! c = PersistedValue.OfCloudFile<VagabondMetadata>(getStoreMetadataPath append id)
            let! md = c.GetValueAsync()
            return Some md

        with :? FileNotFoundException -> return None
    }

    let getAssemblyLoadInfo (id : AssemblyId) = local {
        let! assemblyExists = CloudFile.Exists (getStoreAssemblyPath append id)
        if not assemblyExists then return NotLoaded id
        else
            let! metadata = tryGetCurrentMetadata id
            return
                match metadata with
                | None -> NotLoaded(id)
                | Some md -> Loaded(id, false, md)
    }

    /// upload assembly to blob store
    let uploadAssembly (va : VagabondAssembly) = local {
        let assemblyStorePath = getStoreAssemblyPath append va.Id
        let! assemblyExists = CloudFile.Exists assemblyStorePath

        // 1. Upload assembly image.
        if not assemblyExists then
            /// print upload sizes for given assembly
            let uploadSizes = 
                seq {
                    yield sprintf "IMG %s" (sizeOfFile va.Image)
                    match va.Symbols with
                    | Some s -> yield sprintf "PDB %s" (sizeOfFile s)
                    | None -> ()
                } |> String.concat ", "

            logger.Logf LogLevel.Info "Uploading '%s' [%s]" va.FullName uploadSizes
            let! _ = CloudFile.Upload(va.Image, assemblyStorePath, overwrite = true)
            return ()

            // 2. Upload symbols if applicable.
            match va.Symbols with
            | None -> ()
            | Some symbolsPath ->
                let symbolsStorePath = getStoreSymbolsPath append va.Id
                let! symbolsExist = CloudFile.Exists symbolsStorePath
                if not symbolsExist then
                    let! _ = CloudFile.Upload(symbolsPath, symbolsStorePath, overwrite = true)
                    return ()

        // 3. Upload metadata
        // check current metadata in store
        let! currentMetadata = tryGetCurrentMetadata va.Id

        // detect if metadata in blob store is stale
        let isRequiredUpdate =
            match currentMetadata with
            | None -> true
            | Some md ->
                // require a data dependency whose store generation is older than local
                (md.DataDependencies, va.Metadata.DataDependencies)
                ||> Array.exists2 (fun store local -> local.Generation > store.Generation)

        if not isRequiredUpdate then return Loaded(va.Id, false, va.Metadata) else

        // upload data dependencies
        let files = va.PersistedDataDependencies |> Map.ofArray
        let dataFiles = 
            va.Metadata.DataDependencies 
            |> Seq.filter (fun dd -> match dd.Data with Persisted _ -> true | _ -> false)
            |> Seq.map (fun dd -> dd, files.[dd.Id])
            |> Seq.toArray

        let uploadDataFile (dd : DataDependencyInfo, localPath : string) = local {
            let blobPath = getStoreDataPath append va.Id dd
            let! dataExists = CloudFile.Exists blobPath
            if not dataExists then
                logger.Logf LogLevel.Info "Uploading data dependency '%s' [%s]" dd.Name (sizeOfFile localPath)
                let! _ = CloudFile.Upload(localPath, blobPath, overwrite = true)
                ()
        }

        // only print metadata message if updating data dependencies
        if assemblyExists then logger.Logf LogLevel.Info "Updating metadata for '%s'" va.FullName

        do! dataFiles |> Seq.map uploadDataFile |> Local.Parallel |> Local.Ignore

        // upload metadata record; TODO: use CloudAtom for synchronization?
        let! _ = PersistedValue.New<VagabondMetadata>(va.Metadata, path = getStoreMetadataPath append va.Id)
        return Loaded(va.Id, false, va.Metadata)
    }

    interface IRemoteAssemblyReceiver with
        member x.GetLoadedAssemblyInfo(dependencies: AssemblyId []): Async<AssemblyLoadInfo []> = async {
            return! dependencies |> Seq.map getAssemblyLoadInfo |> Local.Parallel |> imem.RunAsync
        }
        
        member x.PushAssemblies(assemblies: VagabondAssembly []): Async<AssemblyLoadInfo []> =  async {
            return! assemblies |> Seq.map uploadAssembly |> Local.Parallel |> imem.RunAsync
        }


/// File store assembly downloader implementation
[<AutoSerializable(false)>]
type private StoreAssemblyDownloader(config : CloudFileStoreConfiguration, imem : InMemoryRuntime, assemblyContainer : string, logger : ISystemLogger) =
    let append (fileName : string) = config.FileStore.Combine(assemblyContainer, fileName)

    interface IAssemblyDownloader with
        member x.GetImageReader(id: AssemblyId): Async<Stream> = async {
            logger.Logf LogLevel.Info "Downloading '%s'" id.FullName
            return! config.FileStore.BeginRead (getStoreAssemblyPath append id)
        }
        
        member x.TryGetSymbolReader(id: AssemblyId): Async<Stream option> = async {
            let symbolsStorePath = getStoreSymbolsPath append id
            let! exists = config.FileStore.FileExists symbolsStorePath
            if exists then
                let! stream = config.FileStore.BeginRead symbolsStorePath
                return Some stream
            else
                return None
        }
        
        member x.ReadMetadata(id: AssemblyId): Async<VagabondMetadata> = 
            local {
                let! c = PersistedValue.OfCloudFile<VagabondMetadata>(getStoreMetadataPath append id)
                return! c.GetValueAsync()
            } |> imem.RunAsync

        member x.GetPersistedDataDependencyReader(id: AssemblyId, dd : DataDependencyInfo): Async<Stream> = async {
            logger.Logf LogLevel.Info "Downloading data dependency '%s'." dd.Name
            return! config.FileStore.BeginRead(getStoreDataPath append id dd)
        }

[<NoEquality; NoComparison>]
type private AssemblyManagerMsg =
    | Upload of seq<VagabondAssembly> * ReplyChannel<DataDependencyInfo []>
    | Download of seq<AssemblyId> * ReplyChannel<VagabondAssembly []>

/// Assembly manager instance
[<Sealed; AutoSerializable(false)>]
type StoreAssemblyManager private (storeConfig : CloudFileStoreConfiguration, serializer : ISerializer, container : string, logger : ISystemLogger) =
    let imem = InMemoryRuntime.Create(fileConfig = storeConfig, serializer = serializer)
    let uploader = new StoreAssemblyUploader(storeConfig, imem, container, logger)
    let downloader = new StoreAssemblyDownloader(storeConfig, imem, container, logger)

    /// <summary>
    ///     Creates a new StoreAssemblyManager instance with provided cloud resources. 
    /// </summary>
    /// <param name="storeConfig">ResourceRegistry collection.</param>
    /// <param name="container">Containing directory in store for persisting assemblies.</param>
    /// <param name="logger">Logger used by uploader. Defaults to no logging.</param>
    static member Create(storeConfig : CloudFileStoreConfiguration, serializer : ISerializer, container : string, ?logger : ISystemLogger) =
        ignore VagabondRegistry.Instance
        let logger = match logger with Some l -> l | None -> new NullLogger() :> _
        new StoreAssemblyManager(storeConfig, serializer, container, logger)


    /// <summary>
    ///     Asynchronously upload provided dependencies to store.
    /// </summary>
    /// <param name="ids">Assemblies to be uploaded.</param>
    /// <returns>List of data dependencies that failed to be serialized.</returns>
    member __.UploadAssemblies(assemblies : seq<VagabondAssembly>) : Async<DataDependencyInfo []> = async {
        logger.Logf LogLevel.Info "Uploading dependencies"
        let! errors = VagabondRegistry.Instance.SubmitDependencies(uploader, assemblies)
        if errors.Length > 0 then
            let errors = errors |> Seq.map (fun dd -> dd.Name) |> String.concat ", "
            logger.Logf LogLevel.Warning "Failed to upload bindings: %s" errors

        return errors
    }

    /// <summary>
    ///     Asynchronously download provided dependencies from store.
    /// </summary>
    /// <param name="ids">Assembly id's requested for download.</param>
    /// <returns>Vagabond assemblies downloaded to local disk.</returns>
    member __.DownloadAssemblies(ids : seq<AssemblyId>) : Async<VagabondAssembly []> = async {
        return! VagabondRegistry.Instance.DownloadAssemblies(downloader, ids)
    }

    /// Load local assemblies to current AppDomain
    member __.LoadAssemblies(assemblies : seq<VagabondAssembly>) =
        VagabondRegistry.Instance.LoadVagabondAssemblies(assemblies)

    /// Compute dependencies for provided object graph
    member __.ComputeDependencies(graph : 'T) : VagabondAssembly [] =
        VagabondRegistry.Instance.ComputeObjectDependencies(graph, permitCompilation = true, includeNativeDependencies = true) 

    /// <summary>
    ///     Registers a native assembly dependency to client state.
    /// </summary>
    /// <param name="assemblyPath">Path to native assembly.</param>
    member __.RegisterNativeDependency(assemblyPath : string) : VagabondAssembly =
        VagabondRegistry.Instance.RegisterNativeDependency assemblyPath

    /// Gets all native dependencies registered in current instance
    member __.NativeDependencies = VagabondRegistry.Instance.NativeDependencies

    interface IAssemblyManager with
        member x.ComputeDependencies(graph: obj): VagabondAssembly [] =
            x.ComputeDependencies graph 
    
        member x.DownloadAssemblies(ids: seq<AssemblyId>): Async<VagabondAssembly []> = 
            x.DownloadAssemblies(ids)
    
        member x.LoadAssemblies(assemblies: seq<VagabondAssembly>): AssemblyLoadInfo [] = 
            x.LoadAssemblies(assemblies)
    
        member x.NativeDependencies: VagabondAssembly [] = 
            x.NativeDependencies
    
        member x.RegisterNativeDependency(path: string): VagabondAssembly =
            x.RegisterNativeDependency path
    
        member x.UploadAssemblies(assemblies: seq<VagabondAssembly>): Async<unit> = async {
            let! _ = x.UploadAssemblies assemblies
            return ()
        }