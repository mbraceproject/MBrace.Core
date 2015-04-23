namespace MBrace.Runtime.Store

open System
open System.IO
open System.Security.AccessControl
open System.Runtime.Serialization

open MBrace.Core.Internals
open MBrace.Store
open MBrace.Store.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.Utils.Retry

/// Store implementation that uses a filesystem as backend.
[<Sealed; DataContract>]
type FileSystemStore private (rootPath : string) =

    [<DataMember(Name = "RootPath")>]
    let rootPath = rootPath

    let initDir dir =
        retry (RetryPolicy.Retry(2, 0.5<sec>))
                (fun () ->
                    if not <| Directory.Exists dir then
                        Directory.CreateDirectory dir |> ignore)

    let normalize (path : string) =
        if Path.IsPathRooted path then
            let nf = Path.GetFullPath path
            if nf.StartsWith rootPath then nf
            else
                let msg = sprintf "invalid path '%O'." path
                raise <| new FormatException(msg)
        else
            Path.Combine(rootPath, path) |> Path.GetFullPath

    /// <summary>
    ///     Creates a new FileSystemStore instance on given path.
    /// </summary>
    /// <param name="path">Local or UNC path.</param>
    /// <param name="create">Create directory if missing. Defaults to false.</param>
    /// <param name="cleanup">Cleanup directory if it exists. Defaults to false.</param>
    static member Create(path : string, ?create, ?cleanup) =
        let create = defaultArg create false
        let rootPath = Path.GetFullPath path
        if create then
            WorkingDirectory.CreateWorkingDirectory(rootPath, ?cleanup = cleanup)
        elif not <| Directory.Exists rootPath then
            raise <| new DirectoryNotFoundException(rootPath)

        new FileSystemStore(rootPath)

    /// <summary>
    ///     Creates a cloud file system store that can be shared between local processes.
    /// </summary>
    static member CreateSharedLocal() =
        let path = Path.Combine(Path.GetTempPath(), "mbrace-shared", "fileStore")
        FileSystemStore.Create(path, create = true, cleanup = false)

    /// <summary>
    ///     Creates a cloud file system store that is unique to the current process.
    /// </summary>
    static member CreateUniqueLocal() =
        let path = Path.Combine(WorkingDirectory.GetDefaultWorkingDirectoryForProcess(), "localStore")
        FileSystemStore.Create(path, create = true, cleanup = true)

    /// FileSystemStore root path
    member __.RootPath = rootPath

    interface ICloudFileStore with
        member __.Name = "FileSystemStore"
        member __.Id = rootPath
        member __.GetDirectoryName(path : string) = Path.GetDirectoryName path
        member __.GetFileName(path : string) = Path.GetFileName path
        member __.Combine(paths : string []) = Path.Combine paths
        member __.GetRootDirectory () = rootPath
        member __.TryGetFullPath (path : string) = try normalize path |> Some with _ -> None
        member __.GetRandomDirectoryName () = Path.Combine(rootPath, mkUUID())

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
            return Directory.EnumerateFiles(normalize directory) |> Seq.toArray
        }

        member __.DirectoryExists(directory : string) = async {
            return Directory.Exists(normalize directory)
        }

        member __.CreateDirectory(directory : string) = async {
            return Directory.CreateDirectory(normalize directory) |> ignore
        }

        member __.DeleteDirectory(container : string, recursiveDelete : bool) = async {
            return Directory.Delete(normalize container, recursiveDelete)
        }

        member __.EnumerateDirectories(directory) = async {
            return Directory.EnumerateDirectories(normalize directory) |> Seq.toArray
        }

        member __.Write(path : string, writer : Stream -> Async<'R>) = async {
            let path = normalize path
            initDir <| Path.GetDirectoryName path
            use fs = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None)
            return! writer fs
        }

        member __.BeginRead(path : string) = async {
            return new FileStream(normalize path, FileMode.Open, FileAccess.Read, FileShare.Read) :> Stream
        }

        member self.OfStream(source : Stream, target : string) = async {
            let path = normalize target
            initDir <| Path.GetDirectoryName path
            use fs = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None)
            do! source.CopyToAsync fs
        }

        member self.ToStream(source : string, target : Stream) = async {
            use! fs = (self :> ICloudFileStore).BeginRead source
            do! fs.CopyToAsync target
        }