namespace MBrace.Runtime.Store

open System
open System.IO
open System.Security.AccessControl
open System.Runtime.Serialization

open MBrace
open MBrace.Continuation
open MBrace.Store
open MBrace.Runtime
open MBrace.Runtime.Utils
open MBrace.Runtime.Utils.Retry
open MBrace.Runtime.Vagrant

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
        let cleanup = defaultArg cleanup false
        let rootPath = Path.GetFullPath path

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

        new FileSystemStore(rootPath)

    /// <summary>
    ///     Creates a local file system store that can be shared between local processes.
    /// </summary>
    static member CreateSharedLocal() =
        let path = Path.Combine(Path.GetTempPath(), "mbrace-local-fs")
        FileSystemStore.Create(path, create = true, cleanup = false)

    /// <summary>
    ///     Creates a local file system store that is unique to the current process.
    /// </summary>
    static member CreateUniqueLocal() =
        let path = Path.Combine(Path.GetTempPath(), sprintf "mbrace-cache-%d" <| System.Diagnostics.Process.GetCurrentProcess().Id)
        FileSystemStore.Create(path, create = true, cleanup = true)

    interface ICloudFileStore with
        member __.Name = "FileSystemStore"
        member __.Id = rootPath
        member __.GetDirectoryName(path : string) = Path.GetDirectoryName path
        member __.GetFileName(path : string) = Path.GetFileName path
        member __.Combine(paths : string []) = Path.Combine paths
        member __.GetRootDirectory () = rootPath
        member __.TryGetFullPath (path : string) = try normalize path |> Some with _ -> None
        member __.CreateUniqueDirectoryPath () = Path.Combine(rootPath, Guid.NewGuid().ToString("N"))

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


[<AutoSerializable(true) ; Sealed; DataContract>]
type internal FileSystemAtom<'T> (path : string) =

    [<DataMember(Name = "Path")>]
    let path = path

    let rec trap (mode : FileMode) (access : FileAccess) (share : FileShare) =
        let fs = 
            try Some(new FileStream(path, mode, access, share))
            with :? IOException as e when File.Exists path -> None

        match fs with
        | Some fs -> fs
        | None -> trap mode access share
    
    interface ICloudAtom<'T> with
        member __.Id = path
        member __.GetValue () = async {
            use fs = trap FileMode.Open FileAccess.Read FileShare.None
            return VagrantRegistry.Pickler.Deserialize<'T>(fs)
        }

        member __.Update(updater : 'T -> 'T, ?maxRetries : int) = async {
            use fs = trap FileMode.Open FileAccess.ReadWrite FileShare.None
            let value = VagrantRegistry.Pickler.Deserialize<'T>(fs, leaveOpen = true)
            let value' = updater value
            fs.Position <- 0L
            VagrantRegistry.Pickler.Serialize<'T>(fs, value', leaveOpen = false)
        }

        member __.Force(value : 'T) = async {
            use fs = trap FileMode.Open FileAccess.Write FileShare.None
            VagrantRegistry.Pickler.Serialize<'T>(fs, value, leaveOpen = false)
        }

    interface ICloudDisposable with
        member __.Dispose () = cloud { 
            return retry (RetryPolicy.Retry(2, 0.5<sec>)) (fun () -> File.Delete path) 
        }
            
/// File system based atom implementation with pessimistic concurrency.
[<Sealed; DataContract>]
type FileSystemAtomProvider private (rootPath : string) =

    [<DataMember(Name = "rootPath")>]
    let rootPath = rootPath

    let createAtom container (initValue : 'T) =
        let directory = Path.Combine(rootPath, container)
        let path = Path.Combine(directory, Path.GetRandomFileName())

        // populate directory if it doesn't exist
        retry (RetryPolicy.Retry(2, 0.5<sec>))
                (fun () -> 
                    if not <| Directory.Exists directory then 
                        Directory.CreateDirectory directory |> ignore)
        
        use fs = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None)
        in VagrantRegistry.Pickler.Serialize<'T>(fs, initValue)

        new FileSystemAtom<'T>(path)

    /// <summary>
    ///     Creates a new FileSystemAtomProvider instance on given path.
    /// </summary>
    /// <param name="path">Local or UNC path. Defaults to system temp folder.</param>
    /// <param name="create">Create directory if missing. Defaults to false</param>
    /// <param name="cleanup">Cleanup directory if it exists. Defaults to false</param>
    [<Obsolete("File system should not be used for atomic operations.")>]
    static member Create(?path : string, ?create, ?cleanup) =
        let create = defaultArg create false
        let cleanup = defaultArg cleanup false

        let rootPath = 
            match path with
            | Some p -> Path.GetFullPath p
            | None -> Path.Combine(Path.GetTempPath(), "mbrace-atom-store")

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

        new FileSystemAtomProvider(rootPath)

    interface ICloudAtomProvider with
        member __.Name = "FileSystemAtomProvider"
        member __.Id = rootPath
        member __.CreateUniqueContainerName () = System.Guid.NewGuid().ToString("N")
        member __.IsSupportedValue _ = true
        member __.CreateAtom<'T>(container : string, initValue : 'T) = async {
            return createAtom container initValue :> ICloudAtom<'T>
        }

        member __.DisposeContainer (container : string) = async {
            let directory = Path.Combine(rootPath, container)
            return
                retry (RetryPolicy.Retry(3, 0.5<sec>))
                        (fun () -> 
                            if Directory.Exists directory then
                                Directory.Delete(directory, true) |> ignore)
        }