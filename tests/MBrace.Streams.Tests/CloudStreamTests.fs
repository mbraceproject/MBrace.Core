namespace MBrace.Streams.Tests
    #nowarn "0444" // Disable mbrace warnings
    #nowarn "0044" // Nunit obsolete
    open System.Threading
    open System.Linq
    open FsCheck.Fluent
    open NUnit.Framework
    open Nessos.Streams
    open MBrace.Streams
    open MBrace
    open System.IO
    open MBrace.SampleRuntime

    [<TestFixture; AbstractClass>]
    type ``CloudStreams tests`` (config : Configuration) =
        do 
            ThreadPool.SetMinThreads(200, 200) |> ignore

        abstract Evaluate : Cloud<'T> -> 'T

        [<Test>]
        member __.``ofArray`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> CloudStream.ofArray |> CloudStream.length |> __.Evaluate
                let y = xs |> Seq.map ((+)1) |> Seq.length
                Assert.AreEqual(y, int x)).Check(config)

        [<Test>]
        member __.``ofCloudArray`` () =
            Spec.ForAny<int[]>(fun xs ->
                let cloudArray = __.Evaluate <| CloudArray.New(xs) 
                let x = cloudArray |> CloudStream.ofCloudArray |> CloudStream.length |> __.Evaluate
                let y = xs |> Seq.map ((+)1) |> Seq.length
                Assert.AreEqual(y, int x)).Check(config)


        [<Test>]
        member __.``toCloudArray`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> CloudStream.ofArray |> CloudStream.map ((+)1) |> CloudStream.toCloudArray |> __.Evaluate
                let y = xs |> Seq.map ((+)1) |> Seq.toArray
                Assert.AreEqual(y, x.ToArray())).Check(config)

        [<Test>]
        member __.``cache`` () =
            Spec.ForAny<int[]>(fun xs ->
                let cloudArray = __.Evaluate <| CloudArray.New(xs) 
                let cached = CloudStream.cache cloudArray |> __.Evaluate 
                let x = cached |> CloudStream.ofCloudArray |> CloudStream.map  (fun x -> x * x) |> CloudStream.toCloudArray |> __.Evaluate
                let x' = cached |> CloudStream.ofCloudArray |> CloudStream.map (fun x -> x * x) |> CloudStream.toCloudArray |> __.Evaluate
                let y = xs |> Seq.map (fun x -> x * x) |> Seq.toArray
                Assert.AreEqual(y, x.ToArray())
                Assert.AreEqual(x'.ToArray(), x.ToArray())).Check(config)

        [<Test>]
        member __.``subsequent caching`` () =
            Spec.ForAny<int[]>(fun xs ->
                let cloudArray = __.Evaluate <| CloudArray.New(xs) 
                let _ = CloudStream.cache cloudArray |> __.Evaluate 
                let cached = CloudStream.cache cloudArray |> __.Evaluate 
                let x = cached |> CloudStream.ofCloudArray |> CloudStream.map  (fun x -> x * x) |> CloudStream.toCloudArray |> __.Evaluate
                let x' = cached |> CloudStream.ofCloudArray |> CloudStream.map (fun x -> x * x) |> CloudStream.toCloudArray |> __.Evaluate
                let y = xs |> Seq.map (fun x -> x * x) |> Seq.toArray
                Assert.AreEqual(y, x.ToArray())
                Assert.AreEqual(x'.ToArray(), x.ToArray())).Check(config)

        [<Test>]
        member __.``ofCloudFiles`` () =
            Spec.ForAny<string []>(fun (xs : string []) ->
                let cfs = 
                    xs |> Array.map(fun text -> 
                            CloudFile.Create(
                                (fun (stream : Stream) -> 
                                            async {
                                                use sw = new StreamWriter(stream)
                                                sw.Write(text) })))
                    |> Cloud.Parallel
                    |> __.Evaluate

                let x = cfs |> CloudStream.ofCloudFiles MBrace.Streams.CloudFile.ReadAllText
                            |> CloudStream.toArray
                            |> __.Evaluate
                            |> Set.ofArray

                let y = cfs |> Array.map (fun cf -> CloudFile.ReadAllText(cf))
                            |> Cloud.Parallel
                            |> __.Evaluate
                            |> Set.ofArray

                Assert.AreEqual(y, x)).Check(config)

        [<Test>]
        member __.``map`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> CloudStream.ofArray |> CloudStream.map (fun n -> 2 * n) |> CloudStream.toArray |> __.Evaluate
                let y = xs |> Seq.map (fun n -> 2 * n) |> Seq.toArray
                Assert.AreEqual(y, x)).Check(config)

        [<Test>]
        member __.``filter`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> CloudStream.ofArray |> CloudStream.filter (fun n -> n % 2 = 0) |> CloudStream.toArray |> __.Evaluate
                let y = xs |> Seq.filter (fun n -> n % 2 = 0) |> Seq.toArray
                Assert.AreEqual(y, x)).Check(config)


        [<Test>]
        member __.``collect`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> CloudStream.ofArray |> CloudStream.collect (fun n -> [|1..n|] |> Stream.ofArray) |> CloudStream.toArray |> __.Evaluate
                let y = xs |> Seq.collect (fun n -> [|1..n|]) |> Seq.toArray
                Assert.AreEqual(y, x)).Check(config)

        [<Test>]
        member __.``fold`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> CloudStream.ofArray |> CloudStream.map (fun n -> 2 * n) |> CloudStream.fold (+) (+) (fun () -> 0) |> __.Evaluate
                let y = xs |> Seq.map (fun n -> 2 * n) |> Seq.fold (+) 0 
                Assert.AreEqual(y, x)).Check(config)  

        [<Test>]
        member __.``sum`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> CloudStream.ofArray |> CloudStream.map (fun n -> 2 * n) |> CloudStream.sum |> __.Evaluate
                let y = xs |> Seq.map (fun n -> 2 * n) |> Seq.sum
                Assert.AreEqual(y, x)).Check(config)

        [<Test>]
        member __.``length`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> CloudStream.ofArray |> CloudStream.filter (fun n -> n % 2 = 0) |> CloudStream.length |> __.Evaluate
                let y = xs |> Seq.filter (fun n -> n % 2 = 0) |> Seq.length
                Assert.AreEqual(y, int x)).Check(config)


        [<Test>]
        member __.``countBy`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> CloudStream.ofArray |> CloudStream.countBy id |> CloudStream.toArray |> __.Evaluate
                let y = xs |> Seq.countBy id |> Seq.map (fun (k,c) -> k, int64 c) |> Seq.toArray
                Assert.AreEqual(set y, set x)).Check(config)


        [<Test>]
        member __.``sortBy`` () =
            Spec.ForAny<int[]>(fun xs ->
                let x = xs |> CloudStream.ofArray |> CloudStream.sortBy id 10 |> CloudStream.toArray |> __.Evaluate
                let y = (xs |> Seq.sortBy id).Take(10).ToArray()
                Assert.AreEqual(y, x)).Check(config)

        [<Test>]
        member __.``withDegreeOfParallelism`` () =
            Spec.ForAny<int[]>(fun xs -> 
                let r = xs 
                        |> CloudStream.ofArray
                        |> CloudStream.map (fun _ -> System.Diagnostics.Process.GetCurrentProcess().Id)
                        |> CloudStream.withDegreeOfParallelism 1
                        |> CloudStream.toArray
                        |> __.Evaluate
                let x = r
                        |> Set.ofArray
                        |> Seq.length
                if xs.Length = 0 then x = 0
                else x = 1 ).Check(config)


//    [<Category("CloudStreams.RunLocal")>]
//    type ``#1 RunLocal Tests`` () =
//        inherit ``CloudStreams tests`` (Configuration())
//
//        override __.Evaluate(expr : Cloud<'T>) : 'T = MBrace.RunLocal expr
//
//    [<Category("CloudStreams.Cluster")>]
    type ``SampleRuntime Streams Tests`` () =
        inherit ``CloudStreams tests`` (Configuration(MaxNbOfTest = 10))
        
        let currentRuntime : MBraceRuntime option ref = ref None
        
        override __.Evaluate(expr : Cloud<'T>) : 'T = currentRuntime.Value.Value.Run expr

        [<TestFixtureSetUp>]
        member test.InitRuntime() =
            lock currentRuntime (fun () ->
                match currentRuntime.Value with
                | Some runtime -> runtime.KillAllWorkers()
                | None -> ()
            
                //let ver = typeof<MBrace>.Assembly.GetName().Version.ToString(3)
                MBraceRuntime.WorkerExecutable <- Path.Combine(__SOURCE_DIRECTORY__, "../../lib/MBrace.SampleRuntime.exe")
                let runtime = MBraceRuntime.InitLocal(4)
                currentRuntime := Some runtime)

        [<TestFixtureTearDown>]
        member test.FiniRuntime() =
            lock currentRuntime (fun () -> 
                match currentRuntime.Value with
                | None -> invalidOp "No runtime specified in test fixture."
                | Some r -> r.KillAllWorkers() ; currentRuntime := None)