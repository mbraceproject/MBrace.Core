namespace MBrace.Streams

open System
open System.IO
open System.Text

/// Provides a line reader from an input stream.
type internal LineReader(stream : Stream) = 
    let reader = new StreamReader(stream)
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
            if not endOfStream then
                let result = stringBuilder.ToString()
                stringBuilder.Clear() |> ignore
                numberOfBytesRead <- numberOfBytesRead + (int64 <| reader.CurrentEncoding.GetByteCount(result))
                result 
            else null

    /// The total number of bytes read
    member self.NumOfBytesRead = numberOfBytesRead

