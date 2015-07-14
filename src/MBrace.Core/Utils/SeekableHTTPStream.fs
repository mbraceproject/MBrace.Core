namespace MBrace.Core.Internals

open System
open System.IO
open System.Net

// Based on http://codereview.stackexchange.com/questions/70679/seekable-http-range-stream

/// Seekable HTTP Stream implementation
[<Sealed; AutoSerializable(false)>]
type SeekableHTTPStream(url : string) =
    inherit Stream()
    let cacheLength = 1024L * 1024L
    let noDataAvaiable = 0
    let mutable stream = Unchecked.defaultof<MemoryStream>
    let mutable currentChunkNumber = -1L
    let mutable length : int64 option = None
    let mutable isDisposed = false

    member self.Url = url

    member private self.EnsureNotDisposed() = 
        if isDisposed then raise <| new ObjectDisposedException("PartialHTTPStream")
    

    override self.CanRead = self.EnsureNotDisposed(); true
    override self.CanWrite = self.EnsureNotDisposed(); false
    override self.CanSeek = self.EnsureNotDisposed(); true


    override self.Length = 
        self.EnsureNotDisposed()
        match length with
        | Some value -> value
        | None -> 
                let request = HttpWebRequest.CreateHttp(url)
                request.Method <- "HEAD"
                use response = request.GetResponse()
                let contentLength = response.ContentLength
                length <- Some contentLength 
                
                contentLength

    override self.Position 
        with get () = 
            self.EnsureNotDisposed()
            let streamPosition = if stream <> null then stream.Position else 0L
            let position = if currentChunkNumber <> -1L then currentChunkNumber * cacheLength else 0L
            position + streamPosition

        and set (value) = 
            self.EnsureNotDisposed()
            self.Seek(value) |> ignore

    override self.Seek(offset : Int64, origin : SeekOrigin) = 
        self.EnsureNotDisposed()
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
            self.ReadChunk(chunkNumber)
            currentChunkNumber <- chunkNumber

        offset <- offset - currentChunkNumber * cacheLength

        stream.Seek(offset, SeekOrigin.Begin) |> ignore

        self.Position

    member private self.ReadNextChunk() =
        currentChunkNumber <- currentChunkNumber + 1L
        self.ReadChunk(currentChunkNumber)

    member private self.ReadChunk(chunkNumberToRead : Int64) = 
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


    override self.Read(buffer : Byte[], offset : Int32, count : Int32 ) = 
        let mutable count = count
        let mutable offset = offset
        self.EnsureNotDisposed()


        if buffer.Length - offset < count then
            raise <| new ArgumentException("count")

        if stream = null then 
            self.ReadNextChunk()

        if self.Position >= self.Length then 
            noDataAvaiable

        else 
            if self.Position + int64 count > self.Length then
                count <- int (self.Length - self.Position)


            let mutable bytesRead = stream.Read(buffer, offset, count)
            let mutable totalBytesRead = bytesRead
            count <- count - bytesRead

            while count > noDataAvaiable do
                self.ReadNextChunk()
                offset <- offset + bytesRead
                bytesRead <- stream.Read(buffer, offset, count)
                count <- count - bytesRead
                totalBytesRead <- totalBytesRead + bytesRead


            totalBytesRead

    override self.SetLength(_ : Int64) = raise <| new NotImplementedException()
    override self.Write(_ : Byte[], _ : Int32, _ : Int32) = raise <| new NotImplementedException()
    override self.Flush() = self.EnsureNotDisposed()

    override self.Close() = 
        self.EnsureNotDisposed()
        base.Close()
        if stream <> null then
            stream.Close()
        isDisposed <- true