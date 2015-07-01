[<AutoOpen>]
module MBrace.Core.StoreExtensions

open System
open System.IO
open System.Text

open MBrace.Core.Internals

#nowarn "444"

type CloudFile with

    /// <summary>
    ///     Uploads a collection local files to store.
    /// </summary>
    /// <param name="sourcePaths">Local paths to files.</param>
    /// <param name="targetDirectory">Containing directory in cloud store.</param>
    /// <param name="overwrite">Enables overwriting of target file if it exists. Defaults to false.</param>
    static member Upload(sourcePaths : seq<string>, targetDirectory : string, ?overwrite : bool) : Local<CloudFile []> = local {
        let sourcePaths = Seq.toArray sourcePaths
        match sourcePaths |> Array.tryFind (not << File.Exists) with
        | Some notFound -> raise <| new FileNotFoundException(notFound)
        | None -> ()

        let uploadFile (localFile : string) = local {
            let fileName = Path.GetFileName localFile
            let! targetPath = CloudPath.Combine(targetDirectory, fileName)
            return! CloudFile.Upload(localFile, targetPath, ?overwrite = overwrite)
        }

        return!
            sourcePaths
            |> Seq.map uploadFile
            |> Local.Parallel
    }

    /// <summary>
    ///     Asynchronously downloads a collection of cloud files to local disk.
    /// </summary>
    /// <param name="sourcePaths">Paths to files in store.</param>
    /// <param name="targetDirectory">Path to target directory in local disk.</param>
    /// <param name="overwrite">Enables overwriting of target file if it exists. Defaults to false.</param>
    static member Download(sourcePaths : seq<string>, targetDirectory : string, ?overwrite : bool) : Local<string []> = local {
        let download (path : string) = local {
            let localFile = Path.Combine(targetDirectory, Path.GetFileName path)
            do! CloudFile.Download(path, localFile, ?overwrite = overwrite)
            return localFile
        }

        return!
            sourcePaths
            |> Seq.map download
            |> Local.Parallel
    }