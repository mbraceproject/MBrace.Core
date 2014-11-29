[<AutoOpen>]
module Nessos.MBrace.Library.CloudFileUtils

open System
open System.Text
open System.IO

open Nessos.MBrace

type CloudFile with

    /// <summary>
    ///     Reads a CloudFile as a sequence of lines.
    /// </summary>
    /// <param name="file">Input CloudFile.</param>
    /// <param name="encoding">Text encoding.</param>
    [<Cloud>]
    static member ReadLines(file : CloudFile, ?encoding : Encoding) = cloud {
        let reader (stream : Stream) = async {
            let ra = new ResizeArray<string> ()
            use sr = 
                match encoding with
                | None -> new StreamReader(stream)
                | Some e -> new StreamReader(stream, e)

            do while not sr.EndOfStream do
                ra.Add <| sr.ReadLine()

            return ra.ToArray()
        }

        return! CloudFile.Read(file, reader)
    }

    /// <summary>
    ///     Writes a sequence of lines to a given CloudFile path.
    /// </summary>
    /// <param name="lines">Lines to be written.</param>
    /// <param name="encoding">Text encoding.</param>
    /// <param name="path">Path to CloudFile.</param>
    [<Cloud>]
    static member WriteLines(lines : seq<string>, ?encoding : Encoding, ?path : string) = cloud {
        let writer (stream : Stream) = async {
            use sw = 
                match encoding with
                | None -> new StreamWriter(stream)
                | Some e -> new StreamWriter(stream, e)
            for line in lines do
                do! sw.WriteLineAsync(line)
        }

        return! CloudFile.New(writer, ?path = path)
    }

    /// <summary>
    ///     Dump all file contents to a single string.
    /// </summary>
    /// <param name="file">Input CloudFile.</param>
    /// <param name="encoding">Text encoding.</param>
    [<Cloud>]
    static member ReadAllText(file : CloudFile, ?encoding : Encoding) = cloud {
        let reader (stream : Stream) = async {
            use sr = 
                match encoding with
                | None -> new StreamReader(stream)
                | Some e -> new StreamReader(stream, e)
            return sr.ReadToEnd()
        }
        return! CloudFile.Read(file, reader)
    }

    /// <summary>
    ///     Writes string contents to given CloudFile.
    /// </summary>
    /// <param name="text">Input text.</param>
    /// <param name="encoding">Output encoding.</param>
    /// <param name="path">Path to Cloud file.</param>
    [<Cloud>]
    static member WriteAllText(text : string, ?encoding : Encoding, ?path : string) = cloud {
        let writer (stream : Stream) = async {
            use sw = 
                match encoding with
                | None -> new StreamWriter(stream)
                | Some e -> new StreamWriter(stream, e)
            do! sw.WriteLineAsync text
        }
        return! CloudFile.New(writer, ?path = path)
    }
        
    /// <summary>
    ///     Dump the contents of given CloudFile as byte[].
    /// </summary>
    /// <param name="file">Input CloudFile.</param>
    [<Cloud>]
    static member ReadAllBytes(file : CloudFile) = cloud {
        let reader (stream : Stream) = async {
            use ms = new MemoryStream()
            do! stream.CopyToAsync ms
            return ms.ToArray()
        }

        return! CloudFile.Read(file, reader)
    }

    /// <summary>
    ///     Write buffer contents to CloudFile.
    /// </summary>
    /// <param name="buffer">Source buffer.</param>
    /// <param name="path">Path to Cloud file.</param>
    [<Cloud>]
    static member WriteAllBytes(buffer : byte [], ?path : string) = cloud {
        let writer (stream : Stream) = stream.AsyncWrite(buffer, 0, buffer.Length)
        return! CloudFile.New(writer, ?path = path)
    }