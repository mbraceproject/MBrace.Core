namespace MBrace.Runtime.Vagabond

open System
open System.IO
open System.Text.RegularExpressions
open System.Runtime.Serialization

open Nessos.Vagabond
open Nessos.Vagabond.AssemblyProtocols

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Store
open MBrace.Store.Internals
open MBrace.Client
open MBrace.Runtime.Utils

#nowarn "1571"

[<AutoOpen>]
module private Common =

//    /// blob prefix for stored assemblies
//    let prefix = "vagabond"

    /// Gets a unique blob filename for provided assembly
    let filename (id : AssemblyId) = Vagabond.GetFileName id

    let assemblyName append id = append <| filename id + id.Extension
    let symbolsName append id = append <| filename id + ".pdb"
    let metadataName append id = append <| filename id + ".vgb"
    let dataFile append id (dd : DataDependencyInfo) = append <| sprintf "%s-%d-%d.dat" (filename id) dd.Id dd.Generation

///// Assembly to blob store uploader implementation
//type private StoreAssemblyUploader(resource : ResourceRegistry, container : string, logger : ICloudLogger) =
//    let config = resource.Resolve<CloudFileStoreConfiguration>()
//    let imem = LocalRuntime.Create(resource.Register(logger))
//
//    let sizeOfFile (path:string) = FileInfo(path).Length |> getHumanReadableByteSize
//    let append (fileName : string) = config.FileStore.Combine(container, fileName)
//
//    let getAssemblyLoadInfo (id : AssemblyId) = local {
//        let! assemblyExists = CloudFile.Exists (assemblyName append id)
//        if not assemblyExists then return NotLoaded id
//        else
//            let! cell = local {
//                try let! c = CloudValue.OfCloudFile<VagabondMetadata>(metadataName append id) in return Some c
//                with :? FileNotFoundException -> return None
//            }
//            match cell with
//            | None -> return NotLoaded(id)
//            | Some c -> 
//                let! md = c.Value
//                return Loaded(id, false, md)
//    }
//
//    /// upload assembly to blob store
//    let uploadAssembly (va : VagabondAssembly) = local {
//        let assemblyName = assemblyName append va.Id
//        let! assemblyExists = CloudFile.Exists assemblyName
//
//        // 1. Upload assembly image.
//        if not assemblyExists then
//            /// print upload sizes for given assembly
//            let uploadSizes = 
//                seq {
//                    yield sprintf "IMG %s" (sizeOfFile va.Image)
//                    match va.Symbols with
//                    | Some s -> yield sprintf "PDB %s" (sizeOfFile s)
//                    | None -> ()
//                } |> String.concat ", "
//
//            do! Cloud.Logf "Uploading '%s' [%s]" va.FullName uploadSizes
//            do! CloudFile.Upload(va.Image, )
////            let! _ = Blob.UploadFromFile(config, prefix, assemblyName, va.Image)
//            return ()
//
//            // 2. Upload symbols if applicable.
//            match va.Symbols with
//            | None -> ()
//            | Some symbolsPath ->
//                let symbolsName = symbolsName va.Id
//                let! symbolsExist = Blob.Exists(config, prefix, symbolsName)
//                if not symbolsExist then
//                    let! _ = Blob.UploadFromFile(config, prefix, symbolsName, symbolsPath)
//                    return ()
//
//        // 3. Upload metadata
//        // check current metadata in store
//        let metadataName = metadataName va.Id
//        let metadataCell = Blob<VagabondMetadata>.FromPath(config, prefix, metadataName)
//        let! currentMetadata = metadataCell.TryGetValue()
//
//        // detect if metadata in blob store is stale
//        let isRequiredUpdate =
//            match currentMetadata with
//            | None -> true
//            | Some md ->
//                // require a data dependency whose store generation is older than local
//                (md.DataDependencies, va.Metadata.DataDependencies)
//                ||> Array.exists2 (fun store local -> local.Generation > store.Generation)
//
//        if not isRequiredUpdate then return Loaded(va.Id, false, va.Metadata) else
//
//        // upload data dependencies
//        let files = va.PersistedDataDependencies |> Map.ofArray
//        let dataFiles = 
//            va.Metadata.DataDependencies 
//            |> Seq.filter (fun dd -> match dd.Data with Persisted _ -> true | _ -> false)
//            |> Seq.map (fun dd -> dd, files.[dd.Id])
//            |> Seq.toArray
//
//        let uploadDataFile (dd : DataDependencyInfo, localPath : string) = async {
//            let blobPath = dataFile va.Id dd
//            let! dataExists = Blob.Exists(config, prefix, blobPath)
//            if not dataExists then
//                logger.Logf "Uploading data dependency '%s' [%s]" dd.Name (sizeOfFile localPath)
//                let! _ = Blob.UploadFromFile(config, prefix, blobPath, localPath)
//                ()
//        }
//
//        // only print metadata message if updating data dependencies
//        if assemblyExists then logger.Logf "Updating metadata for '%s'" va.FullName
//
//        do! dataFiles |> Seq.map uploadDataFile |> Async.Parallel |> Async.Ignore
//
//        // upload metadata record; TODO: use table store?
//        let! _ = Blob<VagabondMetadata>.Create(config, prefix, metadataName, fun () -> va.Metadata)
//        return Loaded(va.Id, false, va.Metadata)
//    }





//
//    interface IRemoteAssemblyReceiver with
//        member x.GetLoadedAssemblyInfo(dependencies: AssemblyId list): Async<AssemblyLoadInfo list> = async {
//            let! loadInfo = dependencies |> Seq.map getAssemblyLoadInfo |> Async.Parallel
//            return Array.toList loadInfo
//        }
//        
//        member x.PushAssemblies(assemblies: VagabondAssembly list): Async<AssemblyLoadInfo list> =  async {
//            let! loadInfo = assemblies |> Seq.map uploadAssembly |> Async.Parallel
//            return Array.toList loadInfo
//        }
//
//
///// Blob store assembly downloader implementation
//type private BlobAssemblyDownloader(config : ConfigurationId, logger : ICloudLogger) =
//    
//    interface IAssemblyImporter with
//        member x.GetImageReader(id: AssemblyId): Async<Stream> = async {
//            logger.Logf "Downloading '%s'" id.FullName
//            let blob = Blob.FromPath(config, prefix, assemblyName id)
//            return! blob.OpenRead()
//        }
//        
//        member x.TryGetSymbolReader(id: AssemblyId): Async<Stream option> = async {
//            let! exists = Blob.Exists(config, prefix, symbolsName id)
//            if exists then
//                let blob = Blob.FromPath(config, prefix, symbolsName id)
//                let! stream = blob.OpenRead()
//                return Some stream
//            else
//                return None
//        }
//        
//        member x.ReadMetadata(id: AssemblyId): Async<VagabondMetadata> = async {
//            let cell = Blob<VagabondMetadata>.FromPath(config, prefix, metadataName id)
//            return! cell.GetValue()
//        }
//
//        member x.GetPersistedDataDependencyReader(id: AssemblyId, dd : DataDependencyInfo): Async<Stream> = async {
//            logger.Logf "Downloading data dependency '%s'." dd.Name
//            let blob = Blob.FromPath(config, prefix, dataFile id dd)
//            return! blob.OpenRead()
//        }
//
///// Assembly manager instance
//type BlobAssemblyManager private (config : ConfigurationId, logger : ICloudLogger, includeUnmanagedDependencies : bool) = 
//    let uploader = new BlobAssemblyUploader(config, logger)
//    let downloader = new BlobAssemblyDownloader(config, logger)
//
//    /// Upload provided dependencies to store
//    member __.UploadDependencies(ids : seq<AssemblyId>) = async { 
//        logger.Logf "Uploading dependencies"
//        let! errors = VagabondRegistry.Instance.SubmitDependencies(uploader, ids)
//        if errors.Length > 0 then
//            let errors = errors |> Seq.map (fun dd -> dd.Name) |> String.concat ", "
//            logger.Logf "Failed to upload bindings: %s" errors
//    }
//
//    /// Download provided dependencies from store
//    member __.DownloadDependencies(ids : seq<AssemblyId>) = async {
//        return! VagabondRegistry.Instance.ImportAssemblies(downloader, ids)
//    }
//
//    /// Load local assemblies to current AppDomain
//    member __.LoadAssemblies(assemblies : VagabondAssembly list) =
//        VagabondRegistry.Instance.LoadVagabondAssemblies(assemblies)
//
//    /// Compute dependencies for provided object graph
//    member __.ComputeDependencies(graph : 'T) : AssemblyId list =
//        let managedDependencies = VagabondRegistry.Instance.ComputeObjectDependencies(graph, permitCompilation = true) 
//        [
//            yield! managedDependencies |> VagabondRegistry.Instance.GetVagabondAssemblies
//            if includeUnmanagedDependencies then 
//                yield! VagabondRegistry.Instance.NativeDependencies
//        ] |> List.map (fun va -> va.Id)
//
//    /// Creates a new AssemblyManager instance for given store configuration
//    static member Create(config : ConfigurationId, logger : ICloudLogger, ?includeUnmanagedAssemblies : bool) : BlobAssemblyManager = 
//        new BlobAssemblyManager(config, logger, defaultArg includeUnmanagedAssemblies true)
//
//    /// <summary>
//    ///     Registers a native assembly dependency to client state.
//    /// </summary>
//    /// <param name="assemblyPath">Path to native assembly.</param>
//    static member RegisterNativeDependency(assemblyPath : string) : VagabondAssembly =
//        VagabondRegistry.Instance.RegisterNativeDependency assemblyPath
//
//    /// Gets all native dependencies registered in current instance
//    static member NativeDependencies =
//        VagabondRegistry.Instance.NativeDependencies |> List.map (fun m -> m.Image)