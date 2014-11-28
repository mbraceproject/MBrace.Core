namespace Nessos.MBrace.Runtime.Store

open System
open System.IO
open System.Security.AccessControl
open System.Runtime.Serialization

open Nessos.MBrace.Store
open Nessos.MBrace.Runtime
open Nessos.MBrace.Runtime.Utils
open Nessos.MBrace.Runtime.Utils.Retry

/// Store implementation that uses a filesystem as backend.
[<Sealed;AutoSerializable(false)>]
type FileSystemStore private (rootPath : string) =

    static let atomPickler () = VagrantRegistry.Vagrant.Pickler

    let uuid = 
        let uri = Uri(rootPath)
        if uri.IsUnc then sprintf "filesystemstore:%O" uri
        else sprintf "filesystemstore://%s/%s" (System.Net.Dns.GetHostName()) uri.AbsolutePath

    let initDir dir =
        if not <| Directory.Exists dir then
            Directory.CreateDirectory dir |> ignore

    let atomContainer = Path.Combine(rootPath, "_atomic")

    let rec trap path (mode : FileMode) (access : FileAccess) (share : FileShare) =
        let fs = 
            try Some(new FileStream(path, mode, access, share))
            with :? IOException as e when File.Exists(path) -> None

        match fs with
        | Some fs -> fs
        | None -> trap path mode access share

    let getAtomPath(id : string) = Path.Combine(atomContainer, id)
    let getFileSystemPath(file : string) = Path.Combine(rootPath, file)

    let isValidPath (path : string) =
        // need formats of type "container/folder"
        if Path.IsPathRooted path then false
        else
            // best way to verify a path is valid format
            let isValidFormat = try Path.GetFullPath path |> ignore ; true with _ -> false
            if isValidFormat then
                match Path.GetDirectoryName path with
                | "" | null -> false
                | dir -> dir |> Path.GetDirectoryName |> String.IsNullOrEmpty
            else
                false

    /// <summary>
    ///     Creates a new FileSystemStore instance on given path.
    /// </summary>
    /// <param name="path">Local or UNC path.</param>
    /// <param name="create">Create directory if missing. Defaults to false</param>
    /// <param name="cleanup">Cleanup directory if it exists. Defaults to false</param>
    static member Create(path : string, ?create, ?cleanup) =
        let create = defaultArg create false
        let cleanup = defaultArg cleanup false

        let path = Path.GetFullPath path

        if Directory.Exists path then
            if cleanup then
                let cleanup () =
                    Directory.EnumerateDirectories path |> Seq.iter (fun d -> Directory.Delete(d, true))
                    Directory.EnumerateFiles path |> Seq.iter File.Delete
                        
                retry (RetryPolicy.Retry(2, 0.5<sec>)) cleanup

        elif create then
            retry (RetryPolicy.Retry(2, 0.5<sec>)) (fun () -> Directory.CreateDirectory path |> ignore)
        else
            raise <| new DirectoryNotFoundException(path)

        new FileSystemStore(path)

    /// Initializes a FileSystemStore instance on the local system temp path.
    static member LocalTemp =
        let localFsPath = Path.Combine(Path.GetTempPath(), "mbrace-localfs")
        FileSystemStore.Create(localFsPath, create = true, cleanup = false)

    interface ICloudFileStore with
        member __.UUID = uuid
        member __.GetFactory () =
            let path = rootPath
            {
                new ICloudFileStoreFactory with
                    member __.Create () = FileSystemStore.Create(path, false, false) :> ICloudFileStore
            }

        member __.GetFileContainer(path : string) = Path.GetDirectoryName path
        member __.GetFileName(path : string) = Path.GetFileName path
        member __.Combine(container : string, fileName : string) = Path.Combine(container, fileName)
        member __.IsValidPath(path : string) = isValidPath path

        member __.CreateUniqueContainerName() = Guid.NewGuid().ToString("N")
        member __.CreateUniqueFileName(container : string) = Path.Combine(container, Path.GetRandomFileName())

        member __.GetFileSize(path : string) = async {
            return let fI = new FileInfo(getFileSystemPath path) in fI.Length
        }

        member __.FileExists(file : string) = async {
            return File.Exists(getFileSystemPath file)
        }

        member __.DeleteFile(file : string) = async {
            return File.Delete(getFileSystemPath file)
        }

        member __.EnumerateFiles(container : string) = async {
            return 
                Directory.EnumerateFiles(getFileSystemPath container)
                |> Seq.map (fun f -> Path.Combine(container, Path.GetFileName f))
                |> Seq.toArray
        }

        member __.ContainerExists(container : string) = async {
            return Directory.Exists(getFileSystemPath container)
        }

        member __.CreateContainer(container : string) = async {
            return Directory.CreateDirectory(getFileSystemPath container) |> ignore
        }

        member __.DeleteContainer(container : string) = async {
            return Directory.Delete(getFileSystemPath container)
        }

        member __.EnumerateContainers() = async {
            return
                Directory.EnumerateDirectories(rootPath)
                |> Seq.map Path.GetFileName
                |> Seq.toArray
        }

        member __.BeginWrite(path : string) = async {
            return
                if isValidPath path then
                    let container = Path.GetDirectoryName path
                    do initDir <| getFileSystemPath container

                    new FileStream(getFileSystemPath path, FileMode.Create, FileAccess.Write, FileShare.None) :> Stream
                else
                    raise <| DirectoryNotFoundException(sprintf "invalid path '%s'." path)
        }

        member __.BeginRead(path : string) = async {
            return
                if isValidPath path then
                    new FileStream(getFileSystemPath path, FileMode.Open, FileAccess.Read, FileShare.Read) :> Stream
                else
                    raise <| DirectoryNotFoundException(sprintf "invalid path '%s'." path)
        }

        member self.OfStream(source : Stream, target : string) = async {
            use! fs = (self :> ICloudFileStore).BeginWrite target
            do! source.CopyToAsync fs
        }

        member self.ToStream(source : string, target : Stream) = async {
            use! fs = (self :> ICloudFileStore).BeginRead source
            do! fs.CopyToAsync target
        }

    interface ICloudTableStore with
        member __.UUID = uuid
        member __.GetFactory () =
            let path = rootPath
            {
                new ICloudTableStoreFactory with
                    member __.Create () = FileSystemStore.Create(path, false, false) :> ICloudTableStore
            }

        member __.IsSupportedValue _ = true

        member __.Exists(id : string) = async {
            return File.Exists(getAtomPath id)
        }

        member __.Delete(id : string) = async {
            return File.Delete(getAtomPath id)
        }

        member __.Create<'T>(initial : 'T) = async {
            do initDir atomContainer
            let id = Path.GetRandomFileName()
            use fs = new FileStream(getAtomPath id, FileMode.Create, FileAccess.Write, FileShare.None)
            do atomPickler().Serialize(fs, initial)
            return id
        }

        member __.GetValue<'T>(id : string) = async {
            use fs = trap (getAtomPath id) FileMode.Open FileAccess.Read FileShare.None in
            return atomPickler().Deserialize<'T>(fs)
        }

        member __.Update (id : string, updater : 'T -> 'T) = async {
            let path = getAtomPath id
            use fs = trap path FileMode.Open FileAccess.ReadWrite FileShare.None
            let value = atomPickler().Deserialize<'T>(fs)
            let value' = updater value
            fs.Position <- 0L
            atomPickler().Serialize<'T>(fs, value')
        }

        member __.Force (id : string, value : 'T) = async {
            let path = getAtomPath id
            use fs = trap path FileMode.Open FileAccess.Write FileShare.None
            atomPickler().Serialize<'T>(fs, value)
        }

        member __.EnumerateKeys () = async {
            return 
                Directory.EnumerateFiles(atomContainer)
                |> Seq.map Path.GetFileName
                |> Seq.toArray
        }