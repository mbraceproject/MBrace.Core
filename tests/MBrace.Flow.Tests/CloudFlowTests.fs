namespace MBrace.Flow.Tests

#nowarn "0444" // Disable mbrace warnings
open System
open System.Linq
open System.Collections.Generic
open System.IO
open FsCheck
open NUnit.Framework
open MBrace.Flow
open MBrace
open MBrace.Store
open System.Text

[<TestFixture; AbstractClass>]
type ``CloudFlow tests`` () as self =
    let run (workflow : Cloud<'T>) = self.Run(workflow)
    let runLocal (workflow : Cloud<'T>) = self.RunLocal(workflow)

    let mkDummyWorker () = 
        { 
            new obj() with
                override __.Equals y =
                    match y with
                    | :? IWorkerRef as w -> w.Id = "foo"
                    | _ -> false

                // eirik's note to self: *ALWAYS* override .GetHashCode() if using in Seq.groupBy
                override __.GetHashCode() = hash "foo"

            interface IWorkerRef with 
                member __.Id = "foo" 
                member __.Type = "foo"
                member __.ProcessorCount = Environment.ProcessorCount
                member __.CompareTo y =
                    match y with
                    | :? IWorkerRef as w -> compare "foo" w.Id
                    | _ -> invalidArg "y" "invalid comparand"
        }

    abstract Run : Cloud<'T> -> 'T
    abstract RunLocal : Cloud<'T> -> 'T
    abstract FsCheckMaxNumberOfTests : int

    // #region Cloud vector tests

    [<Test>]
    member __.``1. CloudVector : simple cloudvector`` () =
        let inputs = [|1 .. 1000000|]
        let vector = cloud { return! CloudVector.New(inputs, maxPartitionSize = 100000L, enableCaching = false) } |> run
        vector.PartitionCount |> shouldBe (fun c -> c > 10)
        vector.IsCachingSupported |> shouldEqual false
        vector.GetAllPartitions().Length |> shouldEqual vector.PartitionCount
        cloud { return! vector.ToEnumerable() } |> runLocal |> Seq.toArray |> shouldEqual inputs

    [<Test>]
    member __.``1. CloudVector : caching`` () =
        let inputs = [|1 .. 1000000|]
        let vector = cloud { return! CloudVector.New(inputs, maxPartitionSize = 100000L, enableCaching = true) } |> run
        vector.PartitionCount |> shouldBe (fun c -> c > 10)
        vector.IsCachingSupported |> shouldEqual true
        vector.GetAllPartitions().Length |> shouldEqual vector.PartitionCount

        let dummyWorker = mkDummyWorker()

        // attempt to cache remotely
        cloud {
            do! vector.UpdateCacheState(dummyWorker, [|0 .. 4|])
        } |> run


        let worker,indices = cloud { return! vector.GetCacheState() } |> runLocal |> fun c -> c.[0]
        worker |> shouldEqual dummyWorker
        indices |> shouldEqual [|0..4|]

    [<Test>]
    member __.``1. CloudVector : disposal`` () =
        let inputs = [|1 .. 1000000|]
        let vector = cloud { return! CloudVector.New(inputs, maxPartitionSize = 100000L, enableCaching = false) } |> run
        vector.Dispose() |> ignore
        shouldfail(fun () -> cloud { return! vector.ToEnumerable() } |> runLocal |> Seq.iter ignore)

    [<Test>]
    member __.``1. CloudVector : merging`` () =
        let inputs = [|1 .. 1000000|]
        let N = 10
        let vector = cloud { return! CloudVector.New(inputs, maxPartitionSize = 100000L, enableCaching = false) } |> run
        let merged = CloudVector.Merge(Array.init N (fun _ -> vector))
        merged.PartitionCount |> shouldEqual (N * vector.PartitionCount)
        merged.IsCachingSupported |> shouldEqual false
        merged.GetAllPartitions() 
        |> Seq.groupBy (fun p -> p.Path)
        |> Seq.map (fun (_,ps) -> Seq.length ps)
        |> Seq.forall ((=) N)
        |> shouldEqual true

        for i = 0 to merged.PartitionCount - 1 do
            merged.[i].Path |> shouldEqual (vector.[i % vector.PartitionCount].Path)

        cloud { return! merged.ToEnumerable() }
        |> runLocal
        |> Seq.toArray
        |> shouldEqual (Array.init N (fun _ -> inputs) |> Array.concat)

    [<Test>]
    member __.``1. CloudVector : merged caching`` () =
        let inputs = [|1 .. 1000000|]
        let N = 10
        let vector = cloud { return! CloudVector.New(inputs, maxPartitionSize = 100000L, enableCaching = true) } |> run
        let merged = CloudVector.Merge(Array.init N (fun _ -> vector))

        let dummyWorker = mkDummyWorker()

        cloud {
            do! vector.UpdateCacheState(dummyWorker, [|0 .. vector.PartitionCount - 1|])
        } |> run

        let cState = cloud { return! merged.GetCacheState() } |> runLocal //|> fun c -> c.[0]
        let worker, partitions = cState.[0]
        worker |> shouldEqual dummyWorker
        partitions |> shouldEqual [|0 .. merged.PartitionCount - 1|]

    [<Test>]
    member __.``1. CloudVector : merged disposal`` () =
        let inputs = [|1 .. 1000000|]
        let N = 10
        let vector = cloud { return! CloudVector.New(inputs, maxPartitionSize = 100000L, enableCaching = true) } |> run
        let merged = CloudVector.Merge(Array.init N (fun _ -> vector))
        merged.Dispose() |> ignore
        shouldfail(fun () -> cloud { return! vector.ToEnumerable() } |> runLocal |> Seq.iter ignore)

    // #region Streams tests

    [<Test>]
    member __.``2. CloudFlow : ofArray`` () =
        let f(xs : int []) =
            let x = xs |> CloudFlow.ofArray |> CloudFlow.length |> run
            let y = xs |> Seq.map ((+)1) |> Seq.length
            Assert.AreEqual(y, int x)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : ofCloudVector`` () =
        let f(xs : int []) =
            let CloudVector = cloud { return! CloudVector.New(xs, 100L) } |> run
            let x = CloudVector |> CloudFlow.ofCloudVector |> CloudFlow.length |> run
            let y = xs |> Seq.map ((+)1) |> Seq.length
            Assert.AreEqual(y, int x)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)


    [<Test>]
    member __.``2. CloudFlow : toCloudVector`` () =
        let f(xs : int[]) =            
            let x = xs |> CloudFlow.ofArray |> CloudFlow.map ((+)1) |> CloudFlow.toCloudVector |> run
            let y = xs |> Seq.map ((+)1) |> Seq.toArray
            Assert.AreEqual(y, cloud { return! x.ToEnumerable() } |> runLocal)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)


    [<Test>]
    member __.``2. CloudFlow : toCachedCloudVector`` () =
        let f(xs : string[]) =            
            let cv = xs |> CloudFlow.ofArray |> CloudFlow.map (fun x -> new StringBuilder(x)) |> CloudFlow.toCachedCloudVector |> run
            let x = cv |> CloudFlow.ofCloudVector |> CloudFlow.map (fun sb -> sb.GetHashCode()) |> CloudFlow.toArray |> run
            let y = cv |> CloudFlow.ofCloudVector |> CloudFlow.map (fun sb -> sb.GetHashCode()) |> CloudFlow.toArray |> run
            Assert.AreEqual(x, y)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : cache`` () =
        let f(xs : int[]) =
            let v = cloud { return! CloudVector.New(xs, 1024L) } |> run
//            v.Cache() |> run 
            let x = v |> CloudFlow.ofCloudVector |> CloudFlow.map  (fun x -> x * x) |> CloudFlow.toCloudVector |> run
            let x' = v |> CloudFlow.ofCloudVector |> CloudFlow.map (fun x -> x * x) |> CloudFlow.toCloudVector |> run
            let y = xs |> Seq.map (fun x -> x * x) |> Seq.toArray
            
            let _x = cloud { return! x.ToEnumerable() } |> runLocal |> Seq.toArray
            let _x' = cloud { return! x'.ToEnumerable() } |> runLocal |> Seq.toArray
            
            Assert.AreEqual(y, _x)
            Assert.AreEqual(_x', _x)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : ofCloudFiles with ReadAllText`` () =
        let f(xs : string []) =
            let cfs = xs 
                     |> Array.map(fun text -> CloudFile.WriteAllText(text))
                     |> Cloud.Parallel
                     |> run

            let x = cfs |> CloudFlow.ofCloudFiles CloudFileReader.ReadAllText
                        |> CloudFlow.toArray
                        |> run
                        |> Set.ofArray

            let y = cfs |> Array.map (fun f -> __.RunLocal(cloud { return! CloudFile.ReadAllText(f) }))
                        |> Set.ofSeq

            Assert.AreEqual(y, x)
        Check.QuickThrowOnFail(f, 10)

    [<Test>]
    member __.``2. CloudFlow : ofCloudFiles with ReadLines`` () =
        let f(xs : string [][]) =
            let cfs = xs 
                     |> Array.map(fun text -> CloudFile.WriteAllLines(text))
                     |> Cloud.Parallel
                     |> run

            let x = cfs |> CloudFlow.ofCloudFiles CloudFileReader.ReadLines
                        |> CloudFlow.collect id
                        |> CloudFlow.toArray
                        |> run
                        |> Set.ofArray
            
            let y = cfs |> Array.map (fun f -> __.RunLocal(cloud { return! CloudFile.ReadAllLines(f) }))
                        |> Seq.collect id
                        |> Set.ofSeq

            Assert.AreEqual(y, x)
        Check.QuickThrowOnFail(f, 10)

    [<Test>]
    member __.``2. CloudFlow : ofCloudFilesByLine with ReadLines`` () =
        let f(xs : string [][]) =
            let cfs = xs 
                     |> Array.map(fun text -> CloudFile.WriteAllLines(text))
                     |> Cloud.Parallel
                     |> run

            let x = cfs |> CloudFlow.ofCloudFilesByLine
                        |> CloudFlow.toArray
                        |> run
                        |> Set.ofArray
            
            let y = cfs |> Array.map (fun f -> __.RunLocal(cloud { return! CloudFile.ReadAllLines(f) }))
                        |> Seq.collect id
                        |> Set.ofSeq

            Assert.AreEqual(y, x)
        Check.QuickThrowOnFail(f, 10)
    
    
    [<Test>]
    member __.``2. CloudFlow : ofTextFileByLine`` () =
        
        let f(xs : string []) =
            let cf = CloudFile.WriteAllLines(xs) |> run
            let path = cf.Path

            let x = 
                path 
                |> CloudFlow.ofTextFileByLine
                |> CloudFlow.toArray
                |> run
                |> Array.sortBy id
                    
            
            let y = 
                __.RunLocal(cloud { return! CloudFile.ReadLines(cf) })
                |> Seq.sortBy id
                |> Seq.toArray
                    
            Assert.AreEqual(y, x)
        Check.QuickThrowOnFail(f, 10)

    [<Test>]
    member __.``2. CloudFlow : ofCloudFiles with ReadAllLines`` () =
        let f(xs : string [][]) =
            let cfs = xs 
                     |> Array.map(fun text -> CloudFile.WriteAllLines(text))
                     |> Cloud.Parallel
                     |> run

            let x = cfs |> CloudFlow.ofCloudFiles CloudFileReader.ReadAllLines
                        |> CloudFlow.collect (fun lines -> lines :> _)
                        |> CloudFlow.toArray
                        |> run
                        |> Set.ofArray

            let y = cfs |> Array.map (fun f -> __.RunLocal(cloud { return! CloudFile.ReadAllLines(f) }))
                        |> Seq.collect id
                        |> Set.ofSeq

            Assert.AreEqual(y, x)
        Check.QuickThrowOnFail(f, 10)


    [<Test>]
    member __.``2. CloudFlow : map`` () =
        let f(xs : int[]) =
            let x = xs |> CloudFlow.ofArray |> CloudFlow.map (fun n -> 2 * n) |> CloudFlow.toArray |> run
            let y = xs |> Seq.map (fun n -> 2 * n) |> Seq.toArray
            Assert.AreEqual(y, x)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : filter`` () =
        let f(xs : int[]) =
            let x = xs |> CloudFlow.ofArray |> CloudFlow.filter (fun n -> n % 2 = 0) |> CloudFlow.toArray |> run
            let y = xs |> Seq.filter (fun n -> n % 2 = 0) |> Seq.toArray
            Assert.AreEqual(y, x)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)


    [<Test>]
    member __.``2. CloudFlow : collect`` () =
        let f(xs : int[]) =
            let x = xs |> CloudFlow.ofArray |> CloudFlow.collect (fun n -> [|1..n|] :> _) |> CloudFlow.toArray |> run
            let y = xs |> Seq.collect (fun n -> [|1..n|]) |> Seq.toArray
            Assert.AreEqual(y, x)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : fold`` () =
        let f(xs : int[]) =
            let x = xs |> CloudFlow.ofArray |> CloudFlow.map (fun n -> 2 * n) |> CloudFlow.fold (+) (+) (fun () -> 0) |> run
            let y = xs |> Seq.map (fun n -> 2 * n) |> Seq.fold (+) 0 
            Assert.AreEqual(y, x)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : sum`` () =
        let f(xs : int[]) =
            let x = xs |> CloudFlow.ofArray |> CloudFlow.map (fun n -> 2 * n) |> CloudFlow.sum |> run
            let y = xs |> Seq.map (fun n -> 2 * n) |> Seq.sum
            Assert.AreEqual(y, x)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : length`` () =
        let f(xs : int[]) =
            let x = xs |> CloudFlow.ofArray |> CloudFlow.filter (fun n -> n % 2 = 0) |> CloudFlow.length |> run
            let y = xs |> Seq.filter (fun n -> n % 2 = 0) |> Seq.length
            Assert.AreEqual(y, int x)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)


    [<Test>]
    member __.``2. CloudFlow : countBy`` () =
        let f(xs : int[]) =
            let x = xs |> CloudFlow.ofArray |> CloudFlow.countBy id |> CloudFlow.toArray |> run
            let y = xs |> Seq.countBy id |> Seq.map (fun (k,c) -> k, int64 c) |> Seq.toArray
            Assert.AreEqual(set y, set x)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)


    [<Test>]
    member __.``2. CloudFlow : sortBy`` () =
        let f(xs : int[]) =
            let x = xs |> CloudFlow.ofArray |> CloudFlow.sortBy id 10 |> CloudFlow.toArray |> run
            let y = (xs |> Seq.sortBy id).Take(10).ToArray()
            Assert.AreEqual(y, x)
        Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

    [<Test>]
    member __.``2. CloudFlow : withDegreeOfParallelism`` () =
        let f(xs : int[]) = 
            let r = xs 
                    |> CloudFlow.ofArray
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
                let x = xs |> CloudFlow.ofArray |> CloudFlow.tryFind (fun n -> n = 0) |> run
                let y = xs |> Seq.tryFind (fun n -> n = 0) 
                x = y
            Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

        [<Test>]
        member __.``2. CloudFlow : find`` () =
            let f(xs : int[]) =
                let x = try xs |> CloudFlow.ofArray |> CloudFlow.find (fun n -> n = 0) |> run with | :? KeyNotFoundException -> -1
                let y = try xs |> Seq.find (fun n -> n = 0) with | :? KeyNotFoundException -> -1
                x = y
            Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

        [<Test>]
        member __.``2. CloudFlow : tryPick`` () =
            let f(xs : int[]) =
                let x = xs |> CloudFlow.ofArray |> CloudFlow.tryPick (fun n -> if n = 0 then Some n else None) |> run
                let y = xs |> Seq.tryPick (fun n -> if n = 0 then Some n else None) 
                x = y
            Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

        [<Test>]
        member __.``2. CloudFlow : pick`` () =
            let f(xs : int[]) =
                let x = try xs |> CloudFlow.ofArray |> CloudFlow.pick (fun n -> if n = 0 then Some n else None) |> run with | :? KeyNotFoundException -> -1
                let y = try xs |> Seq.pick (fun n -> if n = 0 then Some n else None)  with | :? KeyNotFoundException -> -1
                x = y
            Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)

        [<Test>]
        member __.``2. CloudFlow : exists`` () =
            let f(xs : int[]) =
                let x = xs |> CloudFlow.ofArray |> CloudFlow.exists (fun n -> n = 0) |> run
                let y = xs |> Seq.exists (fun n -> n = 0) 
                x = y
            Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)


        [<Test>]
        member __.``2. CloudFlow : forall`` () =
            let f(xs : int[]) =
                let x = xs |> CloudFlow.ofArray |> CloudFlow.forall (fun n -> n = 0) |> run
                let y = xs |> Seq.forall (fun n -> n = 0) 
                x = y
            Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)


        [<Test>]
        member __.``2. CloudFlow : forall/CloudFiles`` () =
            let f(xs : int []) =
                let cfs = xs 
                         |> Array.map (fun x -> CloudFile.WriteAllText(string x))
                         |> Cloud.Parallel
                         |> run
                let x = cfs |> CloudFlow.ofCloudFiles CloudFileReader.ReadAllText |> CloudFlow.forall (fun x -> Int32.Parse(x) = 0) |> run
                let y = xs |> Seq.forall (fun n -> n = 0) 
                x = y
            Check.QuickThrowOnFail(f, self.FsCheckMaxNumberOfTests)