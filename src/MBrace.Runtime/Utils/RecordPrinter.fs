namespace MBrace.Runtime.Utils.PrettyPrinters

open System

//
// pretty printer for records
//

type private UntypedRecord = (string * string * Align) list // label * entry * alignment

and Align = Left | Center | Right

and [<NoEquality; NoComparison>] Field<'Record> =
    {
        Label : string
        Align : Align
        Getter : 'Record -> string
    }

[<RequireQualifiedAccess>]
module Field =
    /// <summary>
    ///     Define a record field projection.
    /// </summary>
    /// <param name="label">Field label identifier.</param>
    /// <param name="alignment">Field alignment.</param>
    /// <param name="projection">Projection function.</param>
    let create (label : string) alignment (projection : 'Record -> 'Field) =
        { Label = label ; Align = alignment ; Getter = fun r -> (projection r).ToString() }

type Record =

    /// <summary>
    ///     Pretty print a collection of grouped records.
    /// </summary>
    /// <param name="template">Template used for identifying record type fields.</param>
    /// <param name="table">Row Inputs.</param>
    /// <param name="title">Record title.</param>
    /// <param name="useBorders">Render ascii borders. Defaults to false.</param>
    /// <param name="useBorders">Parallelize record field extraction. Defaults to false.</param>
    static member PrettyPrint (template : Field<'Record> list, table : 'Record list list, ?title : string, ?useBorders : bool, ?parallelize : bool) : string =
        let useBorders = defaultArg useBorders false
        let parallelize = defaultArg parallelize false
        let header = template |> List.map (fun f -> f.Label, f.Label, f.Align) : UntypedRecord
        let getLine (r : 'Record) = template |> List.map (fun f -> f.Label, f.Getter r, f.Align)
        let untypedTable = 
            if parallelize then
                table |> List.toArray |> Array.Parallel.map (List.toArray >> Array.Parallel.map getLine >> Array.toList) |> Array.toList
            else
                List.map (List.map getLine) table : UntypedRecord list list

        let padding = 2
        let margin = 1

        let rec traverseEntryLengths (map : Map<string,int>) (line : UntypedRecord) =
            match line with
            | [] -> map
            | (label, value, _) :: rest ->
                let length = defaultArg (map.TryFind label) 0
                let map' = map.Add (label, max length <| value.Length + padding)
                traverseEntryLengths map' rest

        let lengthMap = List.fold traverseEntryLengths Map.empty (header :: (List.concat untypedTable))

        let repeat (times : int) (c : char) = String(c,times)

        let printHorizontalBorder (template : int list) =
            seq {
                yield "+"

                // ------+
                for length in template do
                    yield repeat length '-'
                    yield "+"

                yield "\n"
            }

        let printEntry length align (field : string) =
            let whitespace = length - field.Length // it is given that > 0
            let lPadding, rPadding =
                match align with
                | Left -> margin, whitespace - margin
                | Right -> whitespace - margin, margin
                | Center -> let div = whitespace / 2 in div, whitespace - div

            seq { 
                yield repeat lPadding ' '
                yield field 
                yield repeat rPadding ' '
                if useBorders then yield "|"
            }

        let printRecord (record : UntypedRecord) =
            seq {
                if useBorders then yield "|"

                for (label, value, align) in record do
                    yield! printEntry lengthMap.[label] align value

                yield "\n"
            }

        let printSeparator (record : UntypedRecord) =
            let record' = record |> List.map (fun (label,value,align) -> (label, repeat value.Length '-', align))
            printRecord record'

        let printTitle =
            seq {
                if not useBorders then yield "\n"

                match title with
                | None -> ()
                | Some title ->
                    let totalLength = (lengthMap.Count - 1) + (Map.toSeq lengthMap |> Seq.map snd |> Seq.sum)

                    // print top level separator
                    if useBorders then
                        yield! printHorizontalBorder [totalLength]
                        yield "|"

                    yield! printEntry (max totalLength (title.Length + 1)) Left title
                    yield "\n"
                    
                    if not useBorders then yield "\n"
            }

        seq {
            if useBorders then
                let horizontalBorder = 
                    header 
                    |> List.map (fun (l,_,_) -> lengthMap.[l]) 
                    |> printHorizontalBorder 
                    |> String.concat ""

                yield! printTitle

                yield horizontalBorder
                yield! printRecord header
                yield horizontalBorder

                for group in untypedTable do
                    for entry in group do
                        yield! printRecord entry
                    if group.Length > 0 then yield horizontalBorder
            else
                yield! printTitle
                yield! printRecord header
                yield! printSeparator header

                for entry in List.concat untypedTable do
                    yield! printRecord entry

        } |> String.concat ""


    /// <summary>
    ///     Pretty print a collection of ungrouped records.
    /// </summary>
    /// <param name="template">Template used for identifying record type fields.</param>
    /// <param name="table">Row Inputs.</param>
    /// <param name="title">Record title.</param>
    /// <param name="useBorders">Render ascii borders. Defaults to false.</param>
    /// <param name="useBorders">Parallelize record field extraction. Defaults to false.</param>
    static member PrettyPrint(template : Field<'Record> list, inputs : 'Record list, ?title, ?useBorders : bool, ?parallelize : bool) =
        Record.PrettyPrint(template, [inputs], ?title = title, ?useBorders = useBorders, ?parallelize = parallelize)