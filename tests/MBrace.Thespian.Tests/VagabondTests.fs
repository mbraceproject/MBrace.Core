namespace MBrace.Thespian.Tests

open System
open System.IO

open NUnit.Framework

open Microsoft.FSharp.Compiler.Interactive.Shell
open Microsoft.FSharp.Compiler.SimpleSourceCodeServices

open MBrace.Core.Tests

[<TestFixture>]
module ``MBrace Thespian Vagabond Tests (FSI)`` =

    let is64BitProcess = IntPtr.Size = 8

    let runsOnMono = lazy(Type.GetType("Mono.Runtime") <> null)

    // by default, NUnit copies test assemblies to a temp directory
    // use Directory.GetCurrentDirectory to gain access to the original build directory
    let private buildDirectory = Directory.GetCurrentDirectory()
    let getPathLiteral (path : string) =
        let fullPath =
            if Path.IsPathRooted path then path
            else Path.Combine(buildDirectory, path)

        sprintf "@\"%s\"" fullPath

    type FsiEvaluationSession with
        
        member fsi.AddReferences (paths : string list) =
            let directives = 
                paths 
                |> Seq.map (fun p -> sprintf "#r %s" <| getPathLiteral p)
                |> String.concat "\n"

            fsi.EvalInteraction directives

        member fsi.LoadScript (path : string) =
            let directive = sprintf "#load %s" <| getPathLiteral path
            fsi.EvalInteraction directive

        member fsi.TryEvalExpression(code : string) =
            try fsi.EvalExpression(code)
            with _ -> None

    let shouldEqual (expected : 'T) (result : FsiValue option) =
        match result with
        | None -> raise <| new AssertionException(sprintf "expected %A, got exception." expected)
        | Some value ->
            if not <| typeof<'T>.IsAssignableFrom value.ReflectionType then
                raise <| new AssertionException(sprintf "expected type %O, got %O." typeof<'T> value.ReflectionType)

            match value.ReflectionValue with
            | :? 'T as result when result = expected -> ()
            | result -> raise <| new AssertionException(sprintf "expected %A, got %A." expected result)
            
    type FsiSession private () =
        static let container = ref None

        static member Start () =
            lock container (fun () ->
                match !container with
                | Some _ -> invalidOp "an fsi session is already running."
                | None ->
                    let dummy = new StringReader("")
                    let fsiConfig = FsiEvaluationSession.GetDefaultConfiguration()
                    let fsi = FsiEvaluationSession.Create(fsiConfig, [| "fsi.exe" ; "--noninteractive" |], dummy, Console.Out, Console.Error)
                    container := Some fsi; fsi)

        static member Stop () =
            lock container (fun () ->
                match !container with
                | None -> invalidOp "No fsi sessions are running"
                | Some fsi ->
                    // need a 'stop' operation here
                    container := None)


        static member Value =
            match !container with
            | None -> invalidOp "No fsi session is running."
            | Some fsi -> fsi


    [<TestFixtureSetUp>]
    let initFsiSession () =

        let fsi = FsiSession.Start()
        let thespianExe = __SOURCE_DIRECTORY__ + "../../../bin/MBrace.Thespian.exe"

        // add dependencies

        fsi.AddReferences 
            [
                "MBrace.Core.dll"
                "MBrace.Flow.dll"
                "MBrace.Runtime.Core.dll"
                "FsPickler.dll"
                "Mono.Cecil.dll"
                "Vagabond.dll"
                "Thespian.dll"
                "MBrace.Thespian.exe"
            ]

        fsi.EvalInteraction "open MBrace.Core"
        fsi.EvalInteraction "open MBrace.Library"
        fsi.EvalInteraction "open MBrace.Flow"
        fsi.EvalInteraction "open MBrace.Thespian"
        fsi.EvalInteraction <| "MBraceThespian.WorkerExecutable <- @\"" + thespianExe + "\""
        fsi.EvalInteraction "let cluster = MBraceThespian.InitLocal 2"
        fsi.EvalInteraction "cluster.AttachLogger(new ConsoleLogger())"


    let defineQuotationEvaluator (fsi : FsiEvaluationSession) =
        fsi.EvalInteraction """
            open Microsoft.FSharp.Quotations
            open Microsoft.FSharp.Linq.RuntimeHelpers

            let eval (e : Expr<'T>) = LeafExpressionConverter.EvaluateQuotation e :?> 'T
        """


    [<TestFixtureTearDown>]
    let stopFsiSession () =
        FsiSession.Value.Interrupt()
        FsiSession.Value.EvalInteraction "cluster.KillAllWorkers()"
        FsiSession.Stop()

    [<Test>]
    let ``01. Simple cloud workflow`` () =
        let fsi = FsiSession.Value

        "cloud { return 42 } |> cluster.Run" |> fsi.TryEvalExpression |> shouldEqual 42

    [<Test>]
    let ``02. Simple data dependency`` () =
        let fsi = FsiSession.Value

        "let x = cloud { return 17 + 25 } |> cluster.Run" |> fsi.EvalInteraction

        "cloud { return x } |> cluster.Run" |> fsi.TryEvalExpression |> shouldEqual 42

    [<Test>]
    let ``03. Updating data dependency in single interaction`` () =
        let fsi = FsiSession.Value

        fsi.EvalInteraction """
            let x = ref 0
            for i in 1 .. 10 do
                x := cluster.Run(cloud { return !x + 1 })
        """

        fsi.EvalExpression "!x" |> shouldEqual 10

    [<Test>]
    let ``04. Updating data dependency across interactions`` () =
        let fsi = FsiSession.Value

        "let mutable x = 0" |> fsi.EvalInteraction

        for i in 1 .. 10 do
            fsi.EvalInteraction "x <- x + 1"
            "cloud { return x } |> cluster.Run" |> fsi.EvalExpression |> shouldEqual i


    [<Test>]
    let ``05. Quotation literal`` () =
        let fsi = FsiSession.Value

        defineQuotationEvaluator fsi

        "cloud { return eval <@ if true then 1 else 0 @> } |> cluster.Run" |> fsi.EvalExpression |> shouldEqual 1

    [<Test>]
    let ``06. Cross-slice Quotation literal`` () =
        let fsi = FsiSession.Value

        fsi.EvalInteraction "let x = 41"
        fsi.EvalInteraction "let _ = cluster.Run(cloud { return x })"

        defineQuotationEvaluator fsi
        
        try "cloud { return eval <@ x + 1 @> } |> cluster.Run" |> fsi.EvalExpression |> shouldEqual 42
        with e -> Assert.Inconclusive("This is an expected failure due to restrictions in quotation literal representation in MSIL.")


    [<Test>]
    let ``07. Custom type`` () =
        let fsi = FsiSession.Value

        fsi.EvalInteraction """
            type T = L | B of T * T

            let rec mkBalanced i =
                if i = 0 then L
                else
                    let c = mkBalanced (i-1)
                    B(c,c)

            let rec count (t : T) = cloud {
                match t with
                | L -> return 1
                | B(l,r) ->
                    let! lc,rc = count l <||> count r
                    return 1 + lc + rc
            }
        """

        """
            let t = mkBalanced 5 in
            count t |> cluster.Run
        """ |> fsi.EvalExpression |> shouldEqual 63

    [<Test>]
    let ``08. Persisting custom type to store`` () =
        let fsi = FsiSession.Value

        fsi.EvalInteraction """
            type P = Z | S of P
            
            let rec toInt p = match p with Z -> 0 | S pd -> 1 + toInt pd  
        """

        fsi.EvalInteraction "let cv = cluster.Store.CloudValue.New (S (S (S Z)))"

        fsi.EvalExpression "toInt cv.Value" |> shouldEqual 3

    [<Test>]
    let ``09. Large static data dependency`` () =
        let fsi = FsiSession.Value

        fsi.EvalInteraction "let large = [|1L .. 1000000L|]"

        fsi.EvalExpression "cloud { return large.Length } |> cluster.Run" |> shouldEqual 1000000

    [<Test>]
    let ``10. Large static data dependency updated value`` () =

        let fsi = FsiSession.Value

        fsi.EvalInteraction "let large = [|1L .. 1000000L|]"

        for i in 1L .. 10L do
            fsi.EvalInteraction <| sprintf "large.[499999] <- %dL" i
            fsi.EvalExpression "cloud { return large.[499999] } |> cluster.Run" |> shouldEqual i

    [<Test>]
    let ``11. Sifting large static binding`` () =
        let fsi = FsiSession.Value

        fsi.EvalInteraction "let large = [|1L .. 10000000L|]"

        fsi.EvalInteraction """
            let test (ts : 'T  []) = cloud {
                let! workerCount = Cloud.GetWorkerCount()
                // warmup; ensure cached everywhere before sending actual test
                do! Cloud.ParallelEverywhere(cloud { return ts.GetHashCode() }) |> Cloud.Ignore
                let! hashCodes = Cloud.Parallel [for i in 1 .. 5 * workerCount -> cloud { return ts.GetHashCode() }]
                let uniqueHashes =
                    hashCodes
                    |> Seq.distinct
                    |> Seq.length

                return workerCount = uniqueHashes
            }
        """

        fsi.EvalExpression "test large |> cluster.Run" |> shouldEqual true