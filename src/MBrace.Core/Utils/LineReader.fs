namespace MBrace

open System
open System.Collections
open System.Collections.Generic
open System.IO
open System.Text

/// Provides a line reader from an input stream.
type private LineReader(stream : Stream, ?encoding : Encoding) = 
    let reader = match encoding with None -> new StreamReader(stream) | Some e -> new StreamReader(stream, e)
    let buffer : char [] = Array.zeroCreate 4096
    let mutable posInBuffer : int = -1
    let mutable numOfChars : int = 0
    let mutable endOfStream = false
    let mutable numberOfBytesRead = 0L
    let stringBuilder = new StringBuilder()
    /// Reads a line of characters from the current stream and returns the data as a string.
    member self.ReadLine() : string = 
        if endOfStream then 
            null
        else
            let mutable lineEndFlag = false
            while not lineEndFlag && not endOfStream do
                if posInBuffer = -1 then
                    posInBuffer <- 0
                    numOfChars <- reader.ReadBlock(buffer, posInBuffer, buffer.Length)
                    if numOfChars = 0 then
                        endOfStream <- true
                if not endOfStream then
                    let mutable i = posInBuffer 
                    while not lineEndFlag && i < numOfChars do
                        if buffer.[i] = '\n' then
                            stringBuilder.Append(buffer, posInBuffer, i - posInBuffer) |> ignore
                            lineEndFlag <- true
                            posInBuffer <- i + 1
                            numberOfBytesRead <- numberOfBytesRead + 1L
                        else if buffer.[i] = '\r' then
                            if i + 1 < numOfChars then
                                if buffer.[i + 1] = '\n' then
                                    stringBuilder.Append(buffer, posInBuffer, i - posInBuffer) |> ignore
                                    lineEndFlag <- true
                                    posInBuffer <- i + 2
                                    numberOfBytesRead <- numberOfBytesRead + 2L
                                else
                                    stringBuilder.Append(buffer, posInBuffer, i - posInBuffer) |> ignore
                                    lineEndFlag <- true
                                    posInBuffer <- i + 1
                                    numberOfBytesRead <- numberOfBytesRead + 1L
                            else 
                                let currentChar = char <| reader.Read()
                                if currentChar = '\n' then
                                    stringBuilder.Append(buffer, posInBuffer, i - posInBuffer) |> ignore
                                    lineEndFlag <- true
                                    posInBuffer <- -1
                                    numberOfBytesRead <- numberOfBytesRead + 2L
                                else
                                    stringBuilder.Append(buffer, posInBuffer, i - posInBuffer) |> ignore
                                    lineEndFlag <- true
                                    buffer.[0] <- currentChar
                                    posInBuffer <- -1
                                    numberOfBytesRead <- numberOfBytesRead + 1L
                        i <- i + 1
                
                    if not lineEndFlag then
                        stringBuilder.Append(buffer, posInBuffer, numOfChars - posInBuffer) |> ignore
                    if i = numOfChars then
                        posInBuffer <- -1
            
            let result = stringBuilder.ToString()
            stringBuilder.Clear() |> ignore
            numberOfBytesRead <- numberOfBytesRead + (int64 <| reader.CurrentEncoding.GetByteCount(result))
            result 
            

    /// The total number of bytes read
    member self.BytesRead = numberOfBytesRead


type private LineEnumerator (stream : Stream, beginPos : int64, endPos : int64, ?encoding : Encoding) =
    let mutable currentLine = Unchecked.defaultof<string>
    do 
        if beginPos > endPos || endPos >= stream.Length then raise <| new IndexOutOfRangeException("endPos")
        ignore <| stream.Seek(beginPos, SeekOrigin.Begin)

    let reader = new LineReader(stream)

    let rec readNext () =
        if beginPos + reader.BytesRead <= endPos then
            let line = reader.ReadLine()
            if beginPos = 0L || reader.BytesRead > 0L then
                currentLine <- line
                true
            else
                readNext()
        else
            false

    interface IEnumerator<string> with
        member __.Current = currentLine
        member __.Current = box currentLine
        member __.MoveNext () = readNext ()
        member __.Dispose () = ()
        member __.Reset () = raise <| new NotSupportedException("LineReader")

/// Provides an enumerable implementation that reads text lines within the supplied seek range.
type internal LineEnumerable(stream : Stream, beginPos : int64, endPos : int64, ?encoding : Encoding) =
    interface IEnumerable<string> with
        member __.GetEnumerator() = new LineEnumerator(stream, beginPos, endPos, ?encoding = encoding) :> IEnumerator<string>
        member __.GetEnumerator() = new LineEnumerator(stream, beginPos, endPos, ?encoding = encoding) :> IEnumerator