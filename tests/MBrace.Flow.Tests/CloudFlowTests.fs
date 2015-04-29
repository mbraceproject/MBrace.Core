namespace MBrace.Flow.Tests

#nowarn "0444" // Disable mbrace warnings
open System
open System.Linq
open System.Collections.Generic
open System.IO
open FsCheck
open NUnit.Framework
open MBrace.Flow
open MBrace.Core
open MBrace.Store
open MBrace.Store.Internals
open System.Text

// Helper type
type Separator = N | R | RN

[<TestFixture; AbstractClass>]
type ``CloudFlow tests`` () as self =
    let run (workflow : Cloud<'T>) = self.Run(workflow)
    let runLocally (workflow : Cloud<'T>) = self.RunLocally(workflow)

    abstract Run : Cloud<'T> -> 'T
    abstract RunLocally : Cloud<'T> -> 'T
    abstract FsCheckMaxNumberOfTests : int
    abstract FsCheckMaxNumberOfIOBoundTests : int

    // #region Flow persist tests

    [<Test>]
    member __.``1. PersistedCloudFlow : simple persist`` () =
        let inputs = [|1L .. 1000000L|]
        let vector = inputs |> CloudFlow.OfArray |> CloudFlow.persist |> run
        let workers = Cloud.GetWorkerCount() |> run
        vector.IsCachingEnabled |> shouldEqual false
        vector.PartitionCount |> shouldEqual workers
        cloud { return! vector.ToEnumerable() } |> runLocally |> Seq.toArray |> shouldEqual inputs
        vector |> CloudFlow.sum |> run |> shouldEqual (Array.sum inputs)

    [<Test>]
    member __.``1. PersistedCloudFlow : caching`` () =
        let inputs = [|1L .. 1000000L|]
        let vector = inputs |> CloudFlow.OfArray |> CloudFlow.persistCached |> run
        let workers = Cloud.GetWorkerCount() |> run
        vector.PartitionCount |> shouldEqual workers
        vector.IsCachingEnabled |> shouldEqual true
        cloud { return! vector.ToEnumerable() } |> runLocally |> Seq.toArray |> shouldEqual inputs
        vector |> CloudFlow.sum |> run |> shouldEqual (Array.sum inputs)

    [<Test>]
    member __.``1. PersistedCloudFlow : disposal`` () =
        let inputs = [|1 .. 1000000|]
        let vector = inputs |> CloudFlow.OfArray |> CloudFlow.persist |> run
        vector |> Cloud.Dispose |> run
        shouldfail(fun () -> cloud { return! vector.ToEnumerable() } |> runLocally |> Seq.iter ignore)

//    [<Test>]
//    member __.``1. PersistedCloudFlow : merging`` () =
//        let inputs = [|1 .. 1000000|]
//        let N = 10
//        let vector = inputs |> CloudFlow.OfArray |> CloudFlow.persist |> run
//        let merged = PersistedCloudFlow.Concat(Array.init N (fun _ -> vector))
//        merged.PartitionCount |> shouldEqual (N * vector.PartitionCount)
//        merged.IsCachingEnabled |> shouldEqual false
//        merged.Partitions
//        |> Seq.groupBy (fun p -> p.Path)
//        |> Seq.map (fun (_,ps) -> Seq.length ps)
//        |> Seq.forall ((=) N)
//        |> shouldEqual true
//
//        for i = 0 to merged.PartitionCount - 1 do
//            merged.[i].Path |> shouldEqual (vector.[i % vector.PartitionCount].Path)
//
//        cloud { return! merged.ToEnumerable() }
//        |> runLocally
//        |> Seq.toArray
//        |> shouldEqual (Array.init N (fun _ -> inputs) |> Array.concat)
//
//    [<Test>]
//    member __.``1. PersistedCloudFlow : merged disposal`` () =
//        let inputs = [|1 .. 1000000|]
//        let N = 10
//        let vector = inputs |> CloudFlow.OfArray |> CloudFlow.persist |> run
//        let merged = PersistedCloudFlow.Concat(Array.init N (fun _ -> vector))
//        merged |> Cloud.Dispose |> run
//        shouldfail(fun () -> cloud { return! vector.ToEnumerable() } |> runLocally |> Seq.iter ignore)

    // #region Streams tests

    [<Test>]
    member __.``2. CloudFlow : ofArray`` () =
        let f(xs : int []) =
            let x = xs |> CloudFlow.OfArray |> CloudFlow.length |> run
            let y = xs |> Seq.map ((+)1) |> Seq.length
            Assert.AreEqual(y, int x)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)


    [<Test>]
    member __.``2. CloudFlow : persist`` () =
        let f(xs : int[]) =            
            let x = xs |> CloudFlow.OfArray |> CloudFlow.map ((+)1) |> CloudFlow.persist |> run
            let y = xs |> Seq.map ((+)1) |> Seq.toArray
            Assert.AreEqual(y, cloud { return! x.ToEnumerable() } |> runLocally)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)


    [<Test>]
    member __.``2. CloudFlow : persistCached`` () =
        let f(xs : string[]) =            
            let cv = xs |> CloudFlow.OfArray |> CloudFlow.map (fun x -> new StringBuilder(x)) |> CloudFlow.persistCached |> run
            let x = cv |> CloudFlow.map (fun sb -> sb.GetHashCode()) |> CloudFlow.toArray |> run
            let y = cv |> CloudFlow.map (fun sb -> sb.GetHashCode()) |> CloudFlow.toArray |> run
            Assert.AreEqual(x, y)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : ofSeqs`` () =
        let tester (xs : int [] []) =
            let flowResult =
                xs
                |> CloudFlow.OfSeqs
                |> CloudFlow.map (fun x -> x * x)
                |> CloudFlow.sum
                |> run

            let seqResult =
                xs
                |> Seq.concat
                |> Seq.map (fun x -> x * x)
                |> Seq.sum

            Assert.AreEqual(seqResult, flowResult)

        Check.QuickThrowOnFail(tester, self.FsCheckMaxNumberOfTests)


    [<Test>]
    member __.``2. CloudFlow : ofCloudFiles with custom deserializer`` () =
        let f(xs : string [][]) =
            let cfs = xs 
                     |> Array.map(fun text -> CloudFile.WriteAllLines(text))
                     |> Cloud.Parallel
                     |> run
            let paths = cfs |> Array.map (fun cf -> cf.Path)
            let x =     CloudFlow.OfCloudFiles(paths, (fun (stream : System.IO.Stream) -> seq { yield stream.Length })) 
                        |> CloudFlow.length
                        |> run

            Assert.AreEqual(paths.Length, x)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfIOBoundTests)

    [<Test>]
    member __.``2. CloudFlow : ofCloudFiles with small threshold`` () =
        let file = CloudFile.WriteAllText("Lorem ipsum dolor sit amet") |> runLocally
        let f (size : int) =
            let size = abs size % 200
            let repeatedPaths = Array.init size (fun _ -> file.Path)
            let length =
                CloudFlow.OfCloudFiles(repeatedPaths, (fun (stream : System.IO.Stream) -> seq { yield stream.Length }), sizeThresholdPerCore = 1L)
                |> CloudFlow.length
                |> run

            Assert.AreEqual(int64 size, length)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfIOBoundTests)

    [<Test>]
    member __.``2. CloudFlow : ofCloudFiles with ReadLines`` () =
        let f(xs : string [][]) =
            let cfs = xs 
                     |> Array.map(fun text -> CloudFile.WriteAllLines(text))
                     |> Cloud.Parallel
                     |> run

            let x = cfs |> Array.map (fun cf -> cf.Path)
                        |> CloudFlow.OfCloudFilesByLine
                        |> CloudFlow.toArray
                        |> run
                        |> Set.ofArray
            
            let y = cfs |> Array.map (fun f -> __.RunLocally(cloud { return! CloudFile.ReadAllLines f.Path }))
                        |> Seq.collect id
                        |> Set.ofSeq

            Assert.AreEqual(y, x)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfIOBoundTests)

    [<Test>]
    member __.``2. CloudFlow : ofCloudFilesByLine with ReadLines`` () =
        let f(xs : string [][]) =
            let cfs = xs 
                     |> Array.map(fun text -> CloudFile.WriteAllLines(text))
                     |> Cloud.Parallel
                     |> run

            let x = cfs |> Array.map (fun cf -> cf.Path)
                        |> CloudFlow.OfCloudFilesByLine
                        |> CloudFlow.toArray
                        |> run
                        |> Set.ofArray
            
            let y = cfs |> Array.map (fun f -> __.RunLocally(cloud { return! CloudFile.ReadAllLines f.Path }))
                        |> Seq.collect id
                        |> Set.ofSeq

            Assert.AreEqual(y, x)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfIOBoundTests)
    
    
    [<Test>]
    member __.``2. CloudFlow : OfCloudFileByLine`` () =
        
        let f(xs : string [], separator : Separator) =
            let separator = 
                match separator with
                | N -> "\n" 
                | R -> "\r"
                | RN -> "\r\n"
            let cf = CloudFile.WriteAllText(xs |> String.concat separator) |> run
            let path = cf.Path

            let x = 
                path 
                |> CloudFlow.OfCloudFileByLine
                |> CloudFlow.toArray
                |> run
                |> Array.sortBy id
                    
            
            let y = 
                __.RunLocally(cloud { return! CloudFile.ReadLines cf.Path })
                |> Seq.sortBy id
                |> Seq.toArray
                    
            Assert.AreEqual(y, x)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfIOBoundTests)

    [<Test>]
    member __.``2. CloudFlow : ofCloudFiles with ReadAllLines`` () =
        let f(xs : string [][]) =
            let cfs = xs 
                     |> Array.map(fun text -> CloudFile.WriteAllLines(text))
                     |> Cloud.Parallel
                     |> run

            let x = cfs 
                        |> Array.map (fun cf -> cf.Path)
                        |> CloudFlow.OfCloudFilesByLine
                        |> CloudFlow.toArray
                        |> run
                        |> Set.ofArray

            let y = cfs |> Array.map (fun f -> __.RunLocally(cloud { return! CloudFile.ReadAllLines f.Path }))
                        |> Seq.collect id
                        |> Set.ofSeq

            Assert.AreEqual(y, x)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfIOBoundTests)


    [<Test>]
    member __.``2. CloudFlow : map`` () =
        let f(xs : int[]) =
            let x = xs |> CloudFlow.OfArray |> CloudFlow.map (fun n -> 2 * n) |> CloudFlow.toArray |> run
            let y = xs |> Seq.map (fun n -> 2 * n) |> Seq.toArray
            Assert.AreEqual(y, x)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : filter`` () =
        let f(xs : int[]) =
            let x = xs |> CloudFlow.OfArray |> CloudFlow.filter (fun n -> n % 2 = 0) |> CloudFlow.toArray |> run
            let y = xs |> Seq.filter (fun n -> n % 2 = 0) |> Seq.toArray
            Assert.AreEqual(y, x)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)


    [<Test>]
    member __.``2. CloudFlow : collect`` () =
        let f(xs : int[]) =
            let x = xs |> CloudFlow.OfArray |> CloudFlow.collect (fun n -> [|1..n|]) |> CloudFlow.toArray |> run
            let y = xs |> Seq.collect (fun n -> [|1..n|]) |> Seq.toArray
            Assert.AreEqual(y, x)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : fold`` () =
        let f(xs : int[]) =
            let x = xs |> CloudFlow.OfArray |> CloudFlow.map (fun n -> 2 * n) |> CloudFlow.fold (+) (+) (fun () -> 0) |> run
            let y = xs |> Seq.map (fun n -> 2 * n) |> Seq.fold (+) 0 
            Assert.AreEqual(y, x)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : sum`` () =
        let f(xs : int[]) =
            let x = xs |> CloudFlow.OfArray |> CloudFlow.map (fun n -> 2 * n) |> CloudFlow.sum |> run
            let y = xs |> Seq.map (fun n -> 2 * n) |> Seq.sum
            Assert.AreEqual(y, x)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : length`` () =
        let f(xs : int[]) =
            let x = xs |> CloudFlow.OfArray |> CloudFlow.filter (fun n -> n % 2 = 0) |> CloudFlow.length |> run
            let y = xs |> Seq.filter (fun n -> n % 2 = 0) |> Seq.length
            Assert.AreEqual(y, int x)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)


    [<Test>]
    member __.``2. CloudFlow : countBy`` () =
        let f(xs : int[]) =
            let x = xs |> CloudFlow.OfArray |> CloudFlow.countBy id |> CloudFlow.toArray |> run
            let y = xs |> Seq.countBy id |> Seq.map (fun (k,c) -> k, int64 c) |> Seq.toArray
            Assert.AreEqual(set y, set x)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)


    [<Test>]
    member __.``2. CloudFlow : sortBy`` () =
        let f(xs : int[]) =
            let x = xs |> CloudFlow.OfArray |> CloudFlow.sortBy id 10 |> CloudFlow.toArray |> run
            let y = (xs |> Seq.sortBy id).Take(10).ToArray()
            Assert.AreEqual(y, x)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : maxBy`` () =
        let f(xs : int[]) =
            if Array.isEmpty xs then
                try let _ = xs |> CloudFlow.OfArray |> CloudFlow.maxBy id |> run in false
                with :? System.ArgumentException -> true
            else
                let x = xs |> CloudFlow.OfArray |> CloudFlow.maxBy id  |> run
                let y = xs |> Seq.maxBy id
                x = y

        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : minBy`` () =
        let f(xs : int[]) =
            if Array.isEmpty xs then
                try let _ = xs |> CloudFlow.OfArray |> CloudFlow.minBy id |> run in false
                with :? System.ArgumentException -> true
            else
                let x = xs |> CloudFlow.OfArray |> CloudFlow.minBy id  |> run
                let y = xs |> Seq.minBy id
                x = y

        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : reduce`` () =
        let f(xs : int[]) =
            if Array.isEmpty xs then
                try let _ = xs |> CloudFlow.OfArray |> CloudFlow.reduce (+) |> run in false
                with :? System.ArgumentException -> true
            else
                let x = xs |> CloudFlow.OfArray |> CloudFlow.reduce (+) |> run
                let y = xs |> Seq.reduce (+)
                x = y

        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : reduceBy`` () =
        let f(xs : int []) =
            let x = xs |> CloudFlow.OfArray |> CloudFlow.reduceBy id (+) |> CloudFlow.toArray |> run
            let y = xs |> Seq.groupBy id |> Seq.map (fun (k, vs) -> k, vs |> Seq.reduce (+)) |> Seq.toArray
            Assert.AreEqual(Array.sortBy fst y, Array.sortBy fst x)

        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : averageBy`` () =
        let f(xs : int[]) =
            if Array.isEmpty xs then
                try let _ = xs |> CloudFlow.OfArray |> CloudFlow.averageBy (float) |> run in false
                with :? System.ArgumentException -> true
            else
                let x = xs |> CloudFlow.OfArray |> CloudFlow.averageBy (float) |> run
                let y = xs |> Seq.averageBy (float)
                x = y

        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : groupBy`` () =
        let f(xs : int[]) =
            let x =
                xs
                |> CloudFlow.OfArray
                |> CloudFlow.groupBy id
                |> CloudFlow.map (fun (k, xs) -> k, Seq.length xs)
                |> CloudFlow.toArray
                |> run
            let y =
                xs
                |> Seq.groupBy id
                |> Seq.map (fun (k, xs) -> k, Seq.length xs)
                |> Seq.toArray
            (x |> Array.sortBy fst) = (y |> Array.sortBy fst)

        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : distinctBy`` () =
        let f(xs : int[]) =
            let x =
                xs
                |> CloudFlow.OfArray
                |> CloudFlow.map (fun v -> Math.Abs v)
                |> CloudFlow.distinctBy id
                |> CloudFlow.toArray
                |> run
            let y =
                xs
                |> Seq.map (fun v -> Math.Abs v)
                |> Seq.distinctBy id
                |> Seq.toArray
            (x |> Array.sortBy id) = (y |> Array.sortBy id)

        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : distinct`` () =
        let f(xs : int[]) =
            let x =
                xs
                |> CloudFlow.OfArray
                |> CloudFlow.map (fun v -> Math.Abs v)
                |> CloudFlow.distinctBy id
                |> CloudFlow.toArray
                |> run
            let y =
                xs
                |> Seq.map (fun v -> Math.Abs v)
                |> Seq.distinctBy id
                |> Seq.toArray
            (x |> Array.sortBy id) = (y |> Array.sortBy id)

        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : take`` () =
        let f (xs : int[], n : int) =
            let n = System.Math.Abs(n)
            let x = xs |> CloudFlow.OfArray |> CloudFlow.take n |> CloudFlow.toArray |> run
            Assert.AreEqual(min xs.Length n, x.Length)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : withDegreeOfParallelism`` () =
        let f(xs : int[]) = 
            let r = xs 
                    |> CloudFlow.OfArray
                    |> CloudFlow.map (fun _ -> System.Diagnostics.Process.GetCurrentProcess().Id)
                    |> CloudFlow.withDegreeOfParallelism 1
                    |> CloudFlow.toArray
                    |> run
            let x = r
                    |> Set.ofArray
                    |> Seq.length
            if xs.Length = 0 then x = 0
            else x = 1
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : tryFind`` () =
        let f(xs : int[]) =
            let x = xs |> CloudFlow.OfArray |> CloudFlow.tryFind (fun n -> n = 0) |> run
            let y = xs |> Seq.tryFind (fun n -> n = 0) 
            x = y
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : find`` () =
        let f(xs : int[]) =
            let x = try xs |> CloudFlow.OfArray |> CloudFlow.find (fun n -> n = 0) |> run with | :? KeyNotFoundException -> -1
            let y = try xs |> Seq.find (fun n -> n = 0) with | :? KeyNotFoundException -> -1
            x = y
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : tryPick`` () =
        let f(xs : int[]) =
            let x = xs |> CloudFlow.OfArray |> CloudFlow.tryPick (fun n -> if n = 0 then Some n else None) |> run
            let y = xs |> Seq.tryPick (fun n -> if n = 0 then Some n else None) 
            x = y
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : pick`` () =
        let f(xs : int[]) =
            let x = try xs |> CloudFlow.OfArray |> CloudFlow.pick (fun n -> if n = 0 then Some n else None) |> run with | :? KeyNotFoundException -> -1
            let y = try xs |> Seq.pick (fun n -> if n = 0 then Some n else None)  with | :? KeyNotFoundException -> -1
            x = y
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : exists`` () =
        let f(xs : int[]) =
            let x = xs |> CloudFlow.OfArray |> CloudFlow.exists (fun n -> n = 0) |> run
            let y = xs |> Seq.exists (fun n -> n = 0) 
            x = y
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)


    [<Test>]
    member __.``2. CloudFlow : forall`` () =
        let f(xs : int[]) =
            let x = xs |> CloudFlow.OfArray |> CloudFlow.forall (fun n -> n = 0) |> run
            let y = xs |> Seq.forall (fun n -> n = 0) 
            x = y
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : isEmpty`` () =
        let f(xs : int[]) =
            let x = xs |> CloudFlow.OfArray |> CloudFlow.isEmpty |> run
            let y = xs |> Seq.isEmpty
            x = y

        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : ofCloudChannel`` () =
        let f(_ : int) =
            let x =
                cloud {
                    let! sendPort, receivePort = CloudChannel.New()
                    let! n =
                        Cloud.Choice [
                            cloud { 
                                for i in [|1..1000|] do
                                    do! CloudChannel.Send(sendPort, i)
                                    do! Cloud.Sleep(100)
                                return None
                            };
                            cloud {
                                let! n =  
                                    CloudFlow.OfCloudChannel(receivePort, 1)
                                    |> CloudFlow.take 2
                                    |> CloudFlow.length
                                return Some n
                            }]
                    return Option.get n
                }
                |> run
            x = 2L
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfIOBoundTests)


    [<Test>]
    member __.``2. CloudFlow : toCloudChannel`` () =
        let f(xs : int[]) =
            let sendPort, receivePort = CloudChannel.New() |> run
            let x = 
                xs
                |> CloudFlow.OfArray
                |> CloudFlow.map (fun v -> v + 1)
                |> CloudFlow.toCloudChannel sendPort
                |> run
            let x = 
                cloud {
                    let list = ResizeArray<int>()
                    for x in xs do 
                        let! v = CloudChannel.Receive(receivePort)
                        list.Add(v)
                    return list
                } |> run
            let y = xs |> Seq.map (fun v -> v + 1) |> Seq.toArray
            (set x) = (set y)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfIOBoundTests)
