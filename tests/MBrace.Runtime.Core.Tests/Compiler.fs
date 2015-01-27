namespace MBrace.Runtime.Tests

open MBrace
open MBrace.Runtime.Vagrant
open MBrace.Runtime.Compiler
open MBrace.Tests

open NUnit.Framework

type UnquoteEvaluator () =
    interface IQuotationEvaluator with
        member __.Eval e = Swensen.Unquote.Operators.eval e

[<TestFixture>]
module ``Cloud Compiler Tests`` =

    do VagrantRegistry.Initialize()

    let compiler = CloudCompiler.Init(new UnquoteEvaluator())

    let compile (expr : Quotations.Expr<Cloud<'T>>) = 
        try 
            let c = compiler.Compile expr
            match c.Warnings with
            | [] -> printfn "compilation successful."
            | ws -> printfn "compilation with warnings:\n%s" <| String.concat "\n" ws
            c

        with e -> printfn "%O" e ; reraise ()

    let shouldSucceedCompilation expr = let comp = compile expr in comp.Warnings |> shouldEqual []
    let shouldFailCompilation expr = shouldFailwith<_, CompilerException> (fun () -> compile expr)
    let shouldWarnCompilationWith warn expr = 
        let comp = compile expr 
        comp.Warnings 
        |> List.exists(fun msg -> msg.Contains warn) 
        |> shouldEqual true


    let valueWithoutAttribute = cloud { return 42 }
    let functionWithoutAttribute x = cloud { return x + 1 }

    [<NoWarn>]
    let functionWithNoWarnAttribute x = cloud { return x + 1 }

    [<Cloud>]
    let blockWithCloudAttributeCallingBlockWithCloudAttr () = cloud {
        try
            let! x = Cloud.Parallel [| cloud { return! functionWithoutAttribute 31 } |]

            return x.[0]

        with e ->
            return -1
    }

    [<Cloud>]
    let blockWithNonSerializableBinding () = cloud {
        let! value = cloud { return [|1uy|] }
        let m = new System.IO.MemoryStream(value)
        return m.Length
    }

    [<Cloud>]
    let blockThatContainsNonMonadicNonSerializableBinding () = cloud {
        let! value = cloud { return [| 1uy |] }
        let! length = Cloud.OfAsync <| async { let m = new System.IO.MemoryStream(value) in return m.Length }
        return length
    }

    type CloudObject () =
        let x = ref 0

        [<Cloud>]
        member __.Compute () = cloud { incr x ; return !x }

//    [<Cloud>]
//    let rec blockThatCallsClientApi () = cloud {
//        return
//            let runtime = MBrace.InitLocal 4 in
//            runtime.Run <@ blockThatCallsClientApi () @>
//    }
//
//    [<Cloud>]
//    let rec blockThatCallsClientApi2 n =
//        if n = 0 then 1
//        else
//            MBrace.RunLocal <@ cloud { return n * blockThatCallsClientApi2 (n-1) } @>

    [<Cloud>]
    module Module =

        module NestedModule =
            let nestedWorkflowThatInheritsCloudAttributeFromContainers () = cloud { return 42 }

        let workflowThatInheritsCloudAttributeFromContainers () = cloud { 
            return! NestedModule.nestedWorkflowThatInheritsCloudAttributeFromContainers ()
        }

//    let dummyRuntime = Unchecked.defaultof<Nessos.MBrace.Client.MBraceRuntime>
//
//    [<Cloud>]
//    let blockThatReferencesMBraceClientValue () = cloud {
//        return dummyRuntime.Nodes
//    }
//
//    let invalidBinding = Unchecked.defaultof<int * Nessos.MBrace.Client.MBraceRuntime list option>
//
//    [<Cloud>]
//    let blockThatReferencesCompositeExpressionOfInvalidType = cloud {
//        let foo = fst invalidBinding
//        return foo.GetHashCode()
//    }


    [<Test>]
    let ``Cloud value missing [<Cloud>] attribute`` () =
        shouldWarnCompilationWith "[<Cloud>]" <@ valueWithoutAttribute @>

    [<Test>]
    let ``Cloud function missing [<Cloud>] attribute`` () =
        shouldWarnCompilationWith "[<Cloud>]" <@ functionWithoutAttribute 41 @>

    [<Test>]
    let ``Cloud function with [<NoWarn>] attribute`` () =
        shouldSucceedCompilation <@ functionWithNoWarnAttribute 41 @>

    [<Test>]
    let ``Nested cloud block missing [<Cloud>] attribute`` () =
        shouldWarnCompilationWith "[<Cloud>]" <@ blockWithCloudAttributeCallingBlockWithCloudAttr () @>

    [<Test>]
    let ``Workflow that inherits [<Cloud>] attribute from containing modules`` () =
        shouldSucceedCompilation <@ Module.workflowThatInheritsCloudAttributeFromContainers () @>

    [<Test>]
    let ``Cloud block with non-serializable binding`` () =
        shouldFailCompilation <@ blockWithNonSerializableBinding () @>

    [<Test>]
    let ``Cloud block with non-serializable binding which is non-monadic`` () =
        shouldSucceedCompilation <@ blockThatContainsNonMonadicNonSerializableBinding () @>

    [<Test>]
    let ``Cloud block that is non-static member`` () =
        shouldFailCompilation <@ cloud { let obj = new CloudObject () in return! obj.Compute () } @>

//    [<Test>]
//    let ``Cloud block that calls the MBrace client API`` () =
//        shouldFailCompilation <@ blockThatCallsClientApi () @>
//
//    [<Test>]
//    let ``Cloud block that calls the MBrace client API 2`` () =
//        shouldFailwith<_, CompilerException>(fun () -> blockThatCallsClientApi2 42 |> ignore)
//
//    [<Test>]
//    let ``Cloud block that references MBrace.Client value`` () =
//        shouldFailCompilation <@ blockThatReferencesMBraceClientValue () @>
//
//    [<Test>]
//    let ``Cloud block that captures MBrace.Client object`` () =
//        let computation = MBrace.Compile <@ cloud { return 42 } @>
//
//        shouldFailCompilation <@ cloud { let x = computation.GetHashCode() in return x } @>
//
//    [<Test>]
//    let ``Cloud block that references MBrace.Client type`` () =
//        shouldFailCompilation <@ cloud { return typeof<Nessos.MBrace.Client.MBraceRuntime> } @>
//
//    [<Test>]
//    let ``Cloud block that attempts to update MBraceSettings`` () =
//        shouldFailCompilation <@ cloud { MBraceSettings.MBracedExecutablePath <- "/tmp" } @>

    [<Test>]
    let ``Cloud block that references block closure`` () =
        let block = cloud { return 12 }
        shouldWarnCompilationWith "closure" <@ block @>

    [<Test>]
    let ``Cloud block that references function closure`` () =
        let f x = cloud { return x + 15 }
        shouldWarnCompilationWith "closure" <@ f 27 @>

//    [<Test>]
//    let ``Cloud block that references value with invalid binding`` () =
//        shouldFailCompilation <@ blockThatReferencesCompositeExpressionOfInvalidType @>