namespace MBrace.Runtime.Store

open System
open System.IO
open System.Security.AccessControl
open System.Runtime.Serialization

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.Utils.Retry

/// Store implementation that uses a filesystem as backend.
[<Sealed; DataContract>]
type FileSystemStore private (rootPath : string) =

    [<DataMember(Name = "RootPath")>]
    let rootPath = rootPath

    // IOException will be signifies attempt to perform concurrent writes of file.
    // An exception to this rule is FileNotFoundException, which is a subtype of IOException.
    static let ioConcurrencyPolicy = 
        Policy(fun _ exn ->
            match exn with
            | :? FileNotFoundException -> None
            | :? IOException -> TimeSpan.FromMilliseconds 200. |> Some
            | _ -> None)

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

    static let getETag (path : string) : ETag = 
        let fI = new FileInfo(path)
        let lwt = fI.LastWriteTimeUtc
        let lwtB = BitConverter.GetBytes lwt.Ticks
        let size = BitConverter.GetBytes fI.Length
        Array.append lwtB size |> Convert.ToBase64String

    /// <summary>
    ///     Creates a new FileSystemStore instance on given path.
    /// </summary>
    /// <param name="path">Local or UNC path.</param>
    /// <param name="create">Create directory if missing. Defaults to false.</param>
    /// <param name="cleanup">Cleanup directory if it exists. Defaults to false.</param>
    static member Create(path : string, ?create : bool, ?cleanup : bool) =
        let create = defaultArg create false
        let rootPath = Path.GetFullPath path
        if create then
            ignore <| WorkingDirectory.CreateWorkingDirectory(rootPath, ?cleanup = cleanup)
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
            return 
                try Directory.Delete(normalize container, recursiveDelete)
                with :? DirectoryNotFoundException -> ()
        }

        member __.EnumerateDirectories(directory) = async {
            return Directory.EnumerateDirectories(normalize directory) |> Seq.toArray
        }

        member __.BeginWrite(path : string) = async {
            let path = normalize path
            initDir <| Path.GetDirectoryName path
            return retry ioConcurrencyPolicy (fun () -> new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None) :> Stream)
        }

        member __.BeginRead(path : string) = async {
            return retry ioConcurrencyPolicy (fun () -> new FileStream(normalize path, FileMode.Open, FileAccess.Read, FileShare.Read) :> Stream)
        }

        member self.CopyOfStream(source : Stream, target : string) = async {
            let target = normalize target
            initDir <| Path.GetDirectoryName target
            use fs = retry ioConcurrencyPolicy (fun () -> new FileStream(target, FileMode.Create, FileAccess.Write, FileShare.None))
            do! source.CopyToAsync fs
        }

        member self.CopyToStream(source : string, target : Stream) = async {
            use! fs = (self :> ICloudFileStore).BeginRead source
            do! fs.CopyToAsync target
        }

        member __.TryGetETag (path : string) = async {
            return
                let path = normalize path in
                if File.Exists path then Some(getETag path)
                else None
        }

        member __.WriteETag(path : string, writer : Stream -> Async<'R>) : Async<ETag * 'R> = async {
            let path = normalize path
            initDir <| Path.GetDirectoryName path
            use fs = retry ioConcurrencyPolicy (fun () -> new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None))
            let! r = writer fs
            // flush to disk before closing stream to ensure etag is correct
            if fs.CanWrite then fs.Flush(flushToDisk = true)
            return getETag path, r
        }

        member __.ReadETag(path : string, etag : ETag) = async {
            let path = normalize path
            let fs = retry ioConcurrencyPolicy (fun () -> new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read))
            if etag = getETag path then
                return Some(fs :> Stream)
            else
                return None
        }