namespace MBrace.Core.Internals

open System
open System.IO
open System.Net

/// Seekable HTTP Stream implementation
[<Sealed; AutoSerializable(false)>]
type SeekableHTTPStream(url : string) =
    inherit Stream()
    let mutable request = HttpWebRequest.CreateHttp(url)
    let mutable response = request.GetResponse()
    let mutable stream = response.GetResponseStream()
    let mutable length : int64 option = None
    let mutable position = 0L
    let mutable isDisposed = false

    let ensureNotDisposed() = 
        if isDisposed then raise <| new ObjectDisposedException("SeekableHTTPStream")

    member self.Url = url

    member self.GetLength() = 
        ensureNotDisposed()
        match length with
        | Some value -> value
        | None -> 
            let request = HttpWebRequest.CreateHttp(url)
            request.Method <- "HEAD"
            use response = request.GetResponse() 
            let contentLength = response.ContentLength
            length <- Some contentLength    
            contentLength

    override self.CanRead = ensureNotDisposed(); true
    override self.CanWrite = ensureNotDisposed(); false
    override self.CanSeek = ensureNotDisposed(); true

    override self.Length = self.GetLength()

    override self.Position 
        with get () = position

        and set (value) = 
            ensureNotDisposed()
            stream.Close()
            response.Close()
            request <- HttpWebRequest.CreateHttp(url)
            request.AddRange(value)
            response <- request.GetResponse()
            stream <- response.GetResponseStream()
            position <- value

    override self.Seek(offset : int64, origin : SeekOrigin) = 
        ensureNotDisposed()
        let offset = 
            match origin with
            | SeekOrigin.Begin -> offset
            | SeekOrigin.Current -> position + offset
            | _ -> self.Length + offset
        self.Position <- offset
        offset

    override self.Read(buffer : Byte[], offset : Int32, count : Int32) = 
        ensureNotDisposed()
        let n = stream.Read(buffer, offset, count)
        position <- position + int64 n
        n

    override self.SetLength(_ : Int64) = raise <| new NotSupportedException()
    override self.Write(_ : Byte[], _ : Int32, _ : Int32) = raise <| new NotSupportedException()
    override self.Flush() = ensureNotDisposed()

    override self.Close() = 
        if not isDisposed then
            base.Close()
            if stream <> null then
                stream.Close()
                response.Close()
            isDisposed <- true

    interface IDisposable with
        member self.Dispose () = self.Close()