namespace MBrace.Runtime

open System
open System.IO
open System.Text.RegularExpressions
open System.Runtime.Serialization

open Microsoft.FSharp.Control

open Nessos.Vagabond
open Nessos.Vagabond.AssemblyProtocols

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Store
open MBrace.Store.Internals
open MBrace.Client
open MBrace.Runtime
open MBrace.Runtime.Utils
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
type private StoreAssemblyUploader(config : CloudFileStoreConfiguration, imem : LocalRuntime, container : string) =
    let sizeOfFile (path:string) = FileInfo(path).Length |> getHumanReadableByteSize
    let append (fileName : string) = config.FileStore.Combine(container, fileName)

    let tryGetCurrentMetadata (id : AssemblyId) = local {
        try 
            let! c = CloudValue.OfCloudFile<VagabondMetadata>(getStoreMetadataPath append id, enableCache = false)
            let! md = c.Value
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

            do! Cloud.Logf "Uploading '%s' [%s]" va.FullName uploadSizes
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
                do! Cloud.Logf "Uploading data dependency '%s' [%s]" dd.Name (sizeOfFile localPath)
                let! _ = CloudFile.Upload(localPath, blobPath, overwrite = true)
                ()
        }

        // only print metadata message if updating data dependencies
        if assemblyExists then do! Cloud.Logf "Updating metadata for '%s'" va.FullName

        do! dataFiles |> Seq.map uploadDataFile |> Local.Parallel |> Local.Ignore

        // upload metadata record; TODO: use CloudAtom for synchronization?
        let! _ = CloudValue.New<VagabondMetadata>(va.Metadata, path = getStoreMetadataPath append va.Id)
        return Loaded(va.Id, false, va.Metadata)
    }

    interface IRemoteAssemblyReceiver with
        member x.GetLoadedAssemblyInfo(dependencies: AssemblyId list): Async<AssemblyLoadInfo list> = async {
            let! loadInfo = dependencies |> Seq.map getAssemblyLoadInfo |> Local.Parallel |> imem.RunAsync
            return Array.toList loadInfo
        }
        
        member x.PushAssemblies(assemblies: VagabondAssembly list): Async<AssemblyLoadInfo list> =  async {
            let! loadInfo = assemblies |> Seq.map uploadAssembly |> Local.Parallel |> imem.RunAsync
            return Array.toList loadInfo
        }


/// File store assembly downloader implementation
type private StoreAssemblyDownloader(config : CloudFileStoreConfiguration, imem : LocalRuntime, container : string, logger : ICloudLogger) =
    let append (fileName : string) = config.FileStore.Combine(container, fileName)

    interface IAssemblyImporter with
        member x.GetImageReader(id: AssemblyId): Async<Stream> = async {
            logger.Logf "Downloading '%s'" id.FullName
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
                let! c = CloudValue.OfCloudFile<VagabondMetadata>(getStoreMetadataPath append id, enableCache = false)
                return! c.Value
            } |> imem.RunAsync

        member x.GetPersistedDataDependencyReader(id: AssemblyId, dd : DataDependencyInfo): Async<Stream> = async {
            logger.Logf "Downloading data dependency '%s'." dd.Name
            return! config.FileStore.BeginRead(getStoreDataPath append id dd)
        }

[<NoEquality; NoComparison>]
type private AssemblyManagerMsg =
    | Upload of seq<VagabondAssembly> * ReplyChannel<DataDependencyInfo []>
    | Download of seq<AssemblyId> * ReplyChannel<VagabondAssembly []>

/// Assembly manager instance
[<Sealed; AutoSerializable(false)>]
type StoreAssemblyManager private (storeConfig : CloudFileStoreConfiguration, serializer : ISerializer, container : string, ?logger : ICloudLogger) =
    let logger = match logger with Some l -> l | None -> new NullLogger() :> _
    let imem = LocalRuntime.Create(fileConfig = storeConfig, serializer = serializer, logger = logger)
    let uploader = new StoreAssemblyUploader(storeConfig, imem, container)
    let downloader = new StoreAssemblyDownloader(storeConfig, imem, container, logger)

    let rec loop (inbox : MailboxProcessor<AssemblyManagerMsg>) = async {
        let! msg = inbox.Receive()
        match msg with
        | Upload (assemblies, rc) ->
            try
                logger.Logf "Uploading dependencies"
                let ids = assemblies |> Seq.map (fun va -> va.Id)
                let! errors = VagabondRegistry.Instance.SubmitDependencies(uploader, ids)
                if errors.Length > 0 then
                    let errors = errors |> Seq.map (fun dd -> dd.Name) |> String.concat ", "
                    logger.Logf "Failed to upload bindings: %s" errors

                rc.Reply errors

            with e -> rc.ReplyWithError e

        | Download (ids, rc) ->
            try
                let! vas = VagabondRegistry.Instance.ImportAssemblies(downloader, ids)
                rc.Reply (List.toArray vas)

            with e -> rc.ReplyWithError e

        return! loop inbox
    }

    let cts = new System.Threading.CancellationTokenSource()
    let actor = MailboxProcessor.Start(loop, cancellationToken = cts.Token)

    /// <summary>
    ///     Creates a new StoreAssemblyManager instance with provided cloud resources. 
    /// </summary>
    /// <param name="storeConfig">ResourceRegistry collection.</param>
    /// <param name="container">Containing directory in store for persisting assemblies.</param>
    /// <param name="logger">Logger used by uploader. Defaults to no logging.</param>
    static member Create(storeConfig : CloudFileStoreConfiguration, serializer : ISerializer, container : string, ?logger : ICloudLogger) =
        ignore VagabondRegistry.Instance
        new StoreAssemblyManager(storeConfig, serializer, container, ?logger = logger)


    /// <summary>
    ///     Asynchronously upload provided dependencies to store.
    /// </summary>
    /// <param name="ids">Assemblies to be uploaded.</param>
    /// <returns>List of data dependencies that failed to be serialized.</returns>
    member __.UploadAssemblies(assemblies : seq<VagabondAssembly>) : Async<DataDependencyInfo []> = 
        actor.PostAndAsyncReply(fun ch -> Upload(assemblies, ch))

    /// <summary>
    ///     Asynchronously download provided dependencies from store.
    /// </summary>
    /// <param name="ids">Assembly id's requested for download.</param>
    /// <returns>Vagabond assemblies downloaded to local disk.</returns>
    member __.DownloadAssemblies(ids : seq<AssemblyId>) : Async<VagabondAssembly []> = 
        actor.PostAndAsyncReply(fun ch -> Download(ids, ch))

    /// Load local assemblies to current AppDomain
    member __.LoadAssemblies(assemblies : seq<VagabondAssembly>) =
        VagabondRegistry.Instance.LoadVagabondAssemblies(assemblies) |> Array.ofList

    /// Compute dependencies for provided object graph
    member __.ComputeDependencies(graph : 'T) : VagabondAssembly [] =
        let managedDependencies = VagabondRegistry.Instance.ComputeObjectDependencies(graph, permitCompilation = true) 
        [|
            yield! managedDependencies |> VagabondRegistry.Instance.GetVagabondAssemblies
            yield! VagabondRegistry.Instance.NativeDependencies
        |]

    /// <summary>
    ///     Registers a native assembly dependency to client state.
    /// </summary>
    /// <param name="assemblyPath">Path to native assembly.</param>
    member __.RegisterNativeDependency(assemblyPath : string) : VagabondAssembly =
        VagabondRegistry.Instance.RegisterNativeDependency assemblyPath

    /// Gets all native dependencies registered in current instance
    member __.NativeDependencies = VagabondRegistry.Instance.NativeDependencies |> Array.ofList

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

    interface IDisposable with
        member __.Dispose() = cts.Cancel()