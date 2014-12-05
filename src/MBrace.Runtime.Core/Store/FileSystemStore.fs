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
type FileSystemStore private (rootPath : string, uuid : string) =

    let initDir dir =
        if not <| Directory.Exists dir then
            Directory.CreateDirectory dir |> ignore

//    let atomContainer = Path.Combine(rootPath, "_atomic")

//    let rec trap path (mode : FileMode) (access : FileAccess) (share : FileShare) =
//        let fs = 
//            try Some(new FileStream(path, mode, access, share))
//            with :? IOException as e when File.Exists(path) -> None
//
//        match fs with
//        | Some fs -> fs
//        | None -> trap path mode access share

//    let getAtomPath(id : string) = Path.Combine(atomContainer, id)

    let normalize (path : string) =
        if Path.IsPathRooted path then
            let nf = Path.GetFullPath path
            if nf.StartsWith rootPath then nf
            else
                let msg = sprintf "invalid path '%O'." path
                raise <| new FormatException(msg)
        else
            Path.Combine(rootPath, path) |> Path.GetFullPath



//        match (try Path.GetFullPath fullPath |> Some with _ -> None) with
//        | Some nf when nf.StartsWith rootPath -> None
//        | nf -> nf

        


            
//        // best way to verify a path is valid format
//        match (try Path.GetFullPath path |> Some with _ -> None) with
//        | None -> false
//        | Some p -> p.StartsWith rootPath

//        // need formats of type "container/folder"
//        if Path.IsPathRooted path then false
//        else
//            // best way to verify a path is valid format
//            let isValidFormat = try Path.GetFullPath path |> ignore ; true with _ -> false
//            if isValidFormat then
//                match Path.GetDirectoryName path with
//                | "" | null -> false
//                | dir -> dir |> Path.GetDirectoryName |> String.IsNullOrEmpty
//            else
//                false

    /// <summary>
    ///     Creates a new FileSystemStore instance on given path.
    /// </summary>
    /// <param name="path">Local or UNC path.</param>
    /// <param name="create">Create directory if missing. Defaults to false</param>
    /// <param name="cleanup">Cleanup directory if it exists. Defaults to false</param>
    static member Create(path : string, ?create, ?cleanup) =
        let create = defaultArg create false
        let cleanup = defaultArg cleanup false

        let rootPath = Path.GetFullPath path

        let uuid = 
            let uri = Uri(rootPath)
            if uri.IsUnc then uri.ToString()
            else sprintf "//%s/%s" (System.Net.Dns.GetHostName()) uri.AbsolutePath

        if Directory.Exists rootPath then
            if cleanup then
                let cleanup () =
                    Directory.EnumerateDirectories rootPath |> Seq.iter (fun d -> Directory.Delete(d, true))
                    Directory.EnumerateFiles rootPath |> Seq.iter File.Delete
                        
                retry (RetryPolicy.Retry(2, 0.5<sec>)) cleanup

        elif create then
            retry (RetryPolicy.Retry(2, 0.5<sec>)) (fun () -> Directory.CreateDirectory rootPath |> ignore)
        else
            raise <| new DirectoryNotFoundException(rootPath)

        new FileSystemStore(rootPath, uuid)

    /// Initializes a FileSystemStore instance on the local system temp path.
    static member LocalTemp =
        let localFsPath = Path.Combine(Path.GetTempPath(), "mbrace-localfs")
        FileSystemStore.Create(localFsPath, create = true, cleanup = false)

    interface ICloudFileStore with
        member __.Name = "FileSystemStore"
        member __.Id = uuid
        member __.GetFileStoreDescriptor () =
            let rootPath = rootPath
            let uuid = uuid
            {
                new ICloudFileStoreDescriptor with
                    member __.Name = "FileSystemStore"
                    member __.Id = uuid
                    member __.Recover () = new FileSystemStore(rootPath, uuid) :> ICloudFileStore
            }

        member __.GetDirectoryName(path : string) = Path.GetDirectoryName path
        member __.GetFileName(path : string) = Path.GetFileName path
        member __.Combine(paths : string []) = Path.Combine paths
        member __.GetRootDirectory () = rootPath
        member __.TryGetFullPath (path : string) = try normalize path |> Some with _ -> None
        member __.GetUniqueDirectoryPath () = Path.Combine(rootPath, Guid.NewGuid().ToString("N"))

        member __.GetFileSize(path : string) = async {
            return let fI = new FileInfo(normalize path) in fI.Length
        }

        member __.FileExists(file : string) = async {
            return File.Exists(normalize file)
        }

        member __.DeleteFile(file : string) = async {
            return File.Delete(normalize file)
        }

        member __.EnumerateFiles(directory : string) = async {
            return 
                Directory.EnumerateFiles(normalize directory)
                |> Seq.toArray
        }

        member __.DirectoryExists(directory : string) = async {
            return Directory.Exists(normalize directory)
        }

        member __.CreateDirectory(directory : string) = async {
            return 
                let dI = Directory.CreateDirectory(normalize directory) in
                directory
        }

        member __.DeleteDirectory(container : string, recursiveDelete : bool) = async {
            return Directory.Delete(normalize container, recursiveDelete)
        }

        member __.EnumerateDirectories(directory) = async {
            return
                Directory.EnumerateDirectories(directory)
                |> Seq.map Path.GetFileName
                |> Seq.toArray
        }

        member __.BeginWrite(path : string) = async {
            return new FileStream(normalize path, FileMode.Create, FileAccess.Write, FileShare.None) :> Stream
        }

        member __.BeginRead(path : string) = async {
            return new FileStream(normalize path, FileMode.Open, FileAccess.Read, FileShare.Read) :> Stream
        }

        member self.OfStream(source : Stream, target : string) = async {
            use! fs = (self :> ICloudFileStore).BeginWrite target
            do! source.CopyToAsync fs
        }

        member self.ToStream(source : string, target : Stream) = async {
            use! fs = (self :> ICloudFileStore).BeginRead source
            do! fs.CopyToAsync target
        }

//    interface ICloudTableStore with
//        member __.UUID = uuid
//        member __.GetFactory () =
//            let path = rootPath
//            {
//                new ICloudTableStoreFactory with
//                    member __.Create () = FileSystemStore.Create(path, false, false) :> ICloudTableStore
//            }
//
//        member __.IsSupportedValue _ = true
//
//        member __.Exists(id : string) = async {
//            return File.Exists(getAtomPath id)
//        }
//
//        member __.Delete(id : string) = async {
//            return File.Delete(getAtomPath id)
//        }
//
//        member __.Create<'T>(initial : 'T) = async {
//            do initDir atomContainer
//            let id = Path.GetRandomFileName()
//            use fs = new FileStream(getAtomPath id, FileMode.Create, FileAccess.Write, FileShare.None)
//            do atomPickler().Serialize(fs, initial)
//            return id
//        }
//
//        member __.GetValue<'T>(id : string) = async {
//            use fs = trap (getAtomPath id) FileMode.Open FileAccess.Read FileShare.None in
//            return atomPickler().Deserialize<'T>(fs)
//        }
//
//        member __.Update (id : string, updater : 'T -> 'T) = async {
//            let path = getAtomPath id
//            use fs = trap path FileMode.Open FileAccess.ReadWrite FileShare.None
//            let value = atomPickler().Deserialize<'T>(fs, leaveOpen = true)
//            let value' = updater value
//            fs.Position <- 0L
//            atomPickler().Serialize<'T>(fs, value')
//        }
//
//        member __.Force (id : string, value : 'T) = async {
//            let path = getAtomPath id
//            use fs = trap path FileMode.Open FileAccess.Write FileShare.None
//            atomPickler().Serialize<'T>(fs, value)
//        }
//
//        member __.EnumerateKeys () = async {
//            return 
//                Directory.EnumerateFiles(atomContainer)
//                |> Seq.map Path.GetFileName
//                |> Seq.toArray
//        }