module MBrace.Runtime.Utils.String

open System
open System.Collections.Generic
open System.IO
open System.Text
open System.Text.RegularExpressions

//
// string builder
//

type StringBuilderM = StringBuilder -> unit

type StringBuilderBuilder () =
    member __.Zero () : StringBuilderM = ignore
    member __.Yield (txt : string) : StringBuilderM = fun b -> b.Append txt |> ignore
    member __.Yield (c : char) : StringBuilderM = fun b -> b.Append c |> ignore
//        member __.Yield (o : obj) : StringBuilderM = fun b -> b.Append o |> ignore
    member __.YieldFrom f = f : StringBuilderM

    member __.Combine(f : StringBuilderM, g : StringBuilderM) = fun b -> f b; g b
    member __.Delay (f : unit -> StringBuilderM) = fun b -> f () b
        
    member __.For (xs : 'a seq, f : 'a -> StringBuilderM) =
        fun b ->
            let e = xs.GetEnumerator ()
            while e.MoveNext() do f e.Current b

    member __.While (p : unit -> bool, f : StringBuilderM) =
        fun b -> while p () do f b



let stringB = new StringBuilderBuilder ()

[<RequireQualifiedAccess>]
module String =
    let inline build (f : StringBuilderM) = 
        let b = new StringBuilder ()
        do f b
        b.ToString()

    /// quick 'n' dirty indentation insertion
    let indentWith (prefix : string) (input : string) =
        (prefix + input.Replace(Environment.NewLine, Environment.NewLine + prefix))

    let inline append (sb : StringBuilder) (x : string) = 
        sb.Append x |> ignore



type StringBuilder with
    member b.Printf fmt = Printf.ksprintf (b.Append >> ignore) fmt
    member b.Printfn fmt = Printf.ksprintf (b.AppendLine >> ignore) fmt

let mprintf fmt = Printf.ksprintf (fun txt (b : StringBuilder) -> b.Append txt |> ignore) fmt
let mprintfn fmt = Printf.ksprintf (fun txt (b : StringBuilder) -> b.AppendLine txt |> ignore) fmt


//
//  a base32 encoding scheme: suitable for case-insensitive filesystems
//

[<RequireQualifiedAccess>]
type Convert private () =

    // taken from : http://www.atrevido.net/blog/PermaLink.aspx?guid=debdd47c-9d15-4a2f-a796-99b0449aa8af
    static let encodingIndex = "qaz2wsx3edc4rfv5tgb6yhn7ujm8k9lp"
    static let inverseIndex = encodingIndex |> Seq.mapi (fun i c -> c,i) |> dict

    /// Converts binary data to Base32 (case-insensitive) encoding
    static member BytesToBase32(bytes : byte []) =
        let b = new StringBuilder()
        let mutable hi = 5
        let mutable idx = 0uy
        let mutable i = 0
                
        while i < bytes.Length do
            // do we need to use the next byte?
            if hi > 8 then
                // get the last piece from the current byte, shift it to the right
                // and increment the byte counter
                idx <- bytes.[i] >>> (hi - 5)
                i <- i + 1
                if i <> bytes.Length then
                    // if we are not at the end, get the first piece from
                    // the next byte, clear it and shift it to the left
                    idx <- ((bytes.[i] <<< (16 - hi)) >>> 3) ||| idx

                hi <- hi - 3
            elif hi = 8 then
                idx <- bytes.[i] >>> 3
                i <- i + 1
                hi <- hi - 3
            else
                // simply get the stuff from the current byte
                idx <- (bytes.[i] <<< (8 - hi)) >>> 3
                hi <- hi + 5

            b.Append (encodingIndex.[int idx]) |> ignore

        b.ToString ()

    /// Converts Base32-encoded string to binary data 
    static member Base32ToBytes(encoded : string) =
        let encoded = encoded.ToLower ()
        let numBytes = encoded.Length * 5 / 8
        let bytes = Array.zeroCreate<byte> numBytes

        let inline get i = 
            try inverseIndex.[encoded.[i]]
            with :? KeyNotFoundException -> raise <| new InvalidDataException()

        if encoded.Length < 3 then
            bytes.[0] <- byte (get 0 ||| (get 1 <<< 5))
        else
            let mutable bit_buffer = get 0 ||| (get 1 <<< 5)
            let mutable bits_in_buffer = 10
            let mutable currentCharIndex = 2

            for i = 0 to numBytes - 1 do
                bytes.[i] <- byte bit_buffer
                bit_buffer <- bit_buffer >>> 8
                bits_in_buffer <- bits_in_buffer - 8
                while bits_in_buffer < 8 && currentCharIndex < encoded.Length do
                    bit_buffer <- bit_buffer ||| (get currentCharIndex <<< bits_in_buffer)
                    bits_in_buffer <- bits_in_buffer + 5
                    currentCharIndex <- currentCharIndex + 1

        bytes

    /// <summary>
    ///     Converts string to base32 representation using provided encoding.
    /// </summary>
    /// <param name="text">Input text.</param>
    /// <param name="encoding">Encoding for string binary representation.</param>
    static member StringToBase32(text : string, ?encoding : Encoding) =
        let encoding = match encoding with None -> Encoding.UTF8 | Some e -> e
        let bytes = encoding.GetBytes text
        Convert.BytesToBase32 bytes

    /// <summary>
    ///     Converts base32 string to text using provided encoding.
    /// </summary>
    /// <param name="encoded">Base-32 encoded string.</param>
    /// <param name="encoding">Encoding used for output string.</param>
    static member Base32ToString(encoded : string, ?encoding : Encoding) =
        let encoding = match encoding with None -> Encoding.UTF8 | Some e -> e
        let bytes = Convert.Base32ToBytes encoded
        encoding.GetString bytes


type Guid with
    member g.ToBase32String() = g.ToByteArray() |> Convert.BytesToBase32