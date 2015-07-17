namespace MBrace.Core.Internals

open System
open System.IO
open System.Net

// Based on http://codereview.stackexchange.com/questions/70679/seekable-http-range-stream

/// Seekable HTTP Stream implementation
[<Sealed; AutoSerializable(false)>]
type SeekableHTTPStream(url : string) as self =
    inherit Stream()
    let cacheLength = 1024L * 1024L
    let noDataAvailable = 0
    let mutable stream = Unchecked.defaultof<MemoryStream>
    let mutable currentChunkNumber = -1L
    let mutable length : int64 option = None
    let mutable isDisposed = false

    let ensureNotDisposed() = 
        if isDisposed then raise <| new ObjectDisposedException("SeekableHTTPStream")

    let readChunk(chunkNumberToRead : Int64) = 
        let rangeStart = chunkNumberToRead * cacheLength
        let mutable rangeEnd = rangeStart + cacheLength - 1L
        if rangeStart + cacheLength > self.Length then
            rangeEnd <- self.Length - 1L

        if stream <> null then 
            stream.Close()

        stream <- new MemoryStream(int cacheLength);

        let request = HttpWebRequest.CreateHttp(url)
        request.AddRange(rangeStart, rangeEnd)

        use response = request.GetResponse()
        response.GetResponseStream().CopyTo(stream);

        stream.Position <- 0L

    let readNextChunk() =
        currentChunkNumber <- currentChunkNumber + 1L
        readChunk currentChunkNumber

    member self.Url = url

    member self.GetLengthAsync() = async {
        ensureNotDisposed()
        match length with
        | Some value -> return value
        | None -> 
            let request = HttpWebRequest.CreateHttp(url)
            request.Method <- "HEAD"
            use! response = request.GetResponseAsync().AwaitResultAsync()
            let contentLength = response.ContentLength
            length <- Some contentLength    
            return contentLength
    }

    override self.CanRead = ensureNotDisposed(); true
    override self.CanWrite = ensureNotDisposed(); false
    override self.CanSeek = ensureNotDisposed(); true

    override self.Length = self.GetLengthAsync() |> Async.RunSync

    override self.Position 
        with get () = 
            ensureNotDisposed()
            let streamPosition = if stream <> null then stream.Position else 0L
            let position = if currentChunkNumber <> -1L then currentChunkNumber * cacheLength else 0L
            position + streamPosition

        and set (value) = 
            ensureNotDisposed()
            self.Seek(value) |> ignore

    override self.Seek(offset : Int64, origin : SeekOrigin) = 
        ensureNotDisposed()
        let offset = 
            match origin with
            | SeekOrigin.Begin -> offset
            | SeekOrigin.Current -> self.Position + offset
            | _ -> self.Length + offset

        self.Seek(offset)

    member private self.Seek(offset : Int64) = 
        let mutable offset = offset
        let chunkNumber = offset / cacheLength

        if currentChunkNumber <> chunkNumber then
            readChunk(chunkNumber)
            currentChunkNumber <- chunkNumber

        offset <- offset - currentChunkNumber * cacheLength

        stream.Seek(offset, SeekOrigin.Begin) |> ignore

        self.Position

    override self.Read(buffer : Byte[], offset : Int32, count : Int32 ) = 
        let mutable count = count
        let mutable offset = offset
        ensureNotDisposed()

        if buffer.Length - offset < count then
            raise <| new ArgumentException("count")

        if stream = null then 
            readNextChunk()

        if self.Position >= self.Length then 
            noDataAvailable

        else 
            if self.Position + int64 count > self.Length then
                count <- int (self.Length - self.Position)


            let mutable bytesRead = stream.Read(buffer, offset, count)
            let mutable totalBytesRead = bytesRead
            count <- count - bytesRead

            while count > noDataAvailable do
                readNextChunk()
                offset <- offset + bytesRead
                bytesRead <- stream.Read(buffer, offset, count)
                count <- count - bytesRead
                totalBytesRead <- totalBytesRead + bytesRead


            totalBytesRead

    override self.SetLength(_ : Int64) = raise <| new NotImplementedException()
    override self.Write(_ : Byte[], _ : Int32, _ : Int32) = raise <| new NotImplementedException()
    override self.Flush() = ensureNotDisposed()

    override self.Close() = 
        if not isDisposed then
            base.Close()
            if stream <> null then
                stream.Close()
            isDisposed <- true

    interface IDisposable with
        member self.Dispose () = self.Close()