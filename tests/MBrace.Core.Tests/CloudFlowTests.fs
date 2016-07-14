namespace MBrace.Core.Tests

open System
open System.Threading
open System.Linq
open System.Collections.Generic
open System.Net
open System.Text
open System.IO

open NUnit.Framework
open FsCheck
open Swensen.Unquote.Assertions

open MBrace.Core
open MBrace.Core.BuilderAsyncExtensions
open MBrace.Flow
open MBrace.Flow.Internals


#nowarn "0444" // Disable mbrace warnings

// Helper type
type Separator = N | R | RN

// General-purpose, runtime independent CloudFlow tests
[<TestFixture>]
module ``CloudFlow Core property tests`` =

    [<Test>]
    let ``ResizeArray concatenator`` () =
        let check (inputs : int[][]) =
            let tas = inputs |> Array.map (fun ts -> new ResizeArray<_>(ts))
            let expected = inputs |> Array.concat
            test <@ ResizeArray.concat tas |> Seq.toArray = expected @>

        Check.QuickThrowOnFail check

    [<Test>]
    let ``Partition by size`` () =
        let check (inputs : int64 [], maxSize : int64) =
            let maxSize = 1L + abs maxSize
            let partitions =
                inputs
                |> Partition.partitionBySize (fun i -> async { return abs i }) maxSize
                |> Async.RunSynchronously
        
            test <@ Array.concat partitions = inputs @>

            for p in partitions do
                if Array.sum p > maxSize then
                    test <@ Array.sum p.[0 .. p.Length - 2] <= maxSize @>

        Check.QuickThrowOnFail check


[<TestFixture; AbstractClass>]
type ``CloudFlow tests`` () as self =
    let runOnCloud (workflow : Cloud<'T>) = self.Run(workflow)
    let runOnCurrentProcess (workflow : Cloud<'T>) = self.RunLocally(workflow)

    /// Urls for running HTTP tests
    let testUrls = 
        [| 
            "http://www.textfiles.com/etext/AUTHORS/SHAKESPEARE/shakespeare-alls-11.txt";
            "http://www.textfiles.com/etext/AUTHORS/SHAKESPEARE/shakespeare-antony-23.txt";
            "http://www.textfiles.com/etext/AUTHORS/SHAKESPEARE/shakespeare-as-12.txt";
            "http://www.textfiles.com/etext/AUTHORS/SHAKESPEARE/shakespeare-comedy-7.txt";
            "http://www.textfiles.com/etext/AUTHORS/SHAKESPEARE/shakespeare-coriolanus-24.txt";
        |]

    let getHttpFileLineCount (url : string) = 
        let mutable lineCount = 0L
        let client = new WebClient()
        use stream = client.OpenRead(url)
        use reader = new StreamReader(stream)

        while reader.ReadLine() <> null do
            lineCount <- lineCount + 1L

        lineCount

    /// Run workflow in the runtime under test
    abstract Run : Cloud<'T> -> 'T
    /// Evaluate workflow in the local test process
    abstract RunLocally : Cloud<'T> -> 'T
    /// Run workflow in the runtime under test, returning logs created by the process.
    abstract RunWithLogs : workflow:Cloud<unit> -> string []
    abstract IsSupportedStorageLevel : StorageLevel -> bool
    abstract FsCheckMaxNumberOfTests : int
    abstract FsCheckMaxNumberOfIOBoundTests : int

    // #region Flow persist tests


    [<Test>]
    member __.``1. PersistedCloudFlow : Simple StorageLevel.Disk`` () =
        if __.IsSupportedStorageLevel StorageLevel.Disk then
            let inputs = [|1L .. 1000000L|]
            let persisted = inputs |> CloudFlow.OfArray |> CloudFlow.persist StorageLevel.Disk |> runOnCloud
            let workers = Cloud.GetWorkerCount() |> runOnCloud
            test <@ persisted.StorageLevel = StorageLevel.Disk @>
            test <@ persisted.PartitionCount = workers @>
            test <@ persisted |> CloudFlow.toArray |> runOnCloud = inputs @>
            test <@ persisted.ToEnumerable() |> Seq.toArray = inputs @>

    [<Test>]
    member __.``1. PersistedCloudFlow : Randomized StorageLevel.Disk`` () =
        if __.IsSupportedStorageLevel StorageLevel.Disk then
            let f(xs : int[]) =            
                let x = xs |> CloudFlow.OfArray |> CloudFlow.map ((+)1) |> CloudFlow.persist StorageLevel.Disk |> runOnCloud
                let y = xs |> Seq.map ((+)1) |> Seq.toArray
                Assert.AreEqual(y, x.ToEnumerable() |> Seq.toArray)
            Check.QuickThrowOnFail(f, maxRuns = __.FsCheckMaxNumberOfTests, shrink = false)

    [<Test>]
    member __.``1. PersistedCloudFlow : StorageLevel.Memory`` () =
        if __.IsSupportedStorageLevel StorageLevel.Memory then
            let inputs = [|1L .. 1000000L|]
            let persisted = inputs |> CloudFlow.OfArray |> CloudFlow.cache |> runOnCloud
            let workers = Cloud.GetWorkerCount() |> runOnCloud
            test <@ persisted.StorageLevel = StorageLevel.Memory @>
            test <@ persisted.PartitionCount = workers @>
            test <@ persisted |> CloudFlow.toArray |> runOnCloud = inputs @>

    [<Test>]
    member __.``1. PersistedCloudFlow : disposal`` () =
        let inputs = [|1 .. 1000000|]
        let persisted = inputs |> CloudFlow.OfArray |> CloudFlow.persist StorageLevel.Disk |> runOnCloud
        persisted |> Cloud.Dispose |> runOnCloud
        raises <@ persisted |> CloudFlow.length |> runOnCloud @>

    [<Test>]
    member __.``1. PersistedCloudFlow : StorageLevel.Memory caching`` () =
        if __.IsSupportedStorageLevel StorageLevel.Memory then
            // ensure that initial array is large enough for disk caching to kick in,
            // otherwise persisted fragments will be encapsulated in the reference as an optimization.
            // Limit is currently set to 512KiB, so total size must be 512KiB * cluster size.
            // Arrays have reference equality semantics, compare hashcodes to ensure that caching is taking place.
            let cv = [|1 .. 20|] |> CloudFlow.OfArray |> CloudFlow.map (fun _ -> [|1L .. 1000000L|]) |> CloudFlow.cache |> runOnCloud
            let x = cv |> CloudFlow.map (fun ref -> ref.GetHashCode()) |> CloudFlow.toArray |> runOnCloud
            for i in 1 .. 5 do
                let y = cv |> CloudFlow.map (fun ref -> ref.GetHashCode()) |> CloudFlow.toArray |> runOnCloud
                Assert.AreEqual(x, y)

    [<Test>]
    member __.``1. PersistedCloudFlow : Simple StorageLevel.MemorySerialized`` () =
        if __.IsSupportedStorageLevel StorageLevel.MemorySerialized then
            let f(xs : int[]) =            
                let pf = xs |> CloudFlow.OfArray |> CloudFlow.map ((+)1) |> CloudFlow.persist StorageLevel.MemorySerialized |> runOnCloud
                let ys = pf |> CloudFlow.length |> runOnCloud
                Assert.AreEqual(ys, xs.LongLength)
            Check.QuickThrowOnFail(f, maxRuns = __.FsCheckMaxNumberOfTests, shrink = false)

    [<Test>]
    member __.``1. PersistedCloudFlow : merging`` () =
        let inputs = [|1 .. 1000000|]
        let N = 10
        let persisted = inputs |> CloudFlow.OfArray |> CloudFlow.persist StorageLevel.MemoryAndDisk |> runOnCloud
        let merged = PersistedCloudFlow.Concat(Array.init N (fun _ -> persisted))
        test <@ merged.PartitionCount = N * persisted.PartitionCount @>
        
        let partitions = persisted.GetPartitions() |> Array.map snd
        let mergedPartitions = merged.GetPartitions() |> Array.map snd
        
        test 
            <@
                mergedPartitions
                |> Seq.countBy (fun p -> p.Id)
                |> Seq.forall (fun (_,n) -> n = N)
            @>

        for i = 0 to mergedPartitions.Length - 1 do
            test <@ mergedPartitions.[i].Id = partitions.[i % persisted.PartitionCount].Id @>

        test <@ merged.ToEnumerable() |> Seq.toArray = (Array.init N (fun _ -> inputs) |> Array.concat) @>

    [<Test>]
    member __.``1. PersistedCloudFlow : merged disposal`` () =
        let inputs = [|1 .. 1000000|]
        let N = 10
        let persisted = inputs |> CloudFlow.OfArray |> CloudFlow.persist StorageLevel.MemoryAndDisk |> runOnCloud
        let merged = PersistedCloudFlow.Concat(Array.init N (fun _ -> persisted))
        merged |> Cloud.Dispose |> runOnCloud
        raises <@ persisted.ToEnumerable() |> Seq.length @>

    // #region Streams tests

    [<Test>]
    member __.``2. CloudFlow : ofArray`` () =
        let f(xs : int []) =
            let x = xs |> CloudFlow.OfArray |> CloudFlow.length |> runOnCloud
            let y = xs |> Seq.map ((+)1) |> Seq.length
            Assert.AreEqual(y, int x)
        Check.QuickThrowOnFail(f, maxRuns = __.FsCheckMaxNumberOfTests, shrink = false)

    [<Test>]
    member __.``2. CloudFlow : ofSeqs`` () =
        let tester (xs : int [] []) =
            let flowResult =
                xs
                |> CloudFlow.OfSeqs
                |> CloudFlow.map (fun x -> x * x)
                |> CloudFlow.sum
                |> runOnCloud

            let seqResult =
                xs
                |> Seq.concat
                |> Seq.map (fun x -> x * x)
                |> Seq.sum

            Assert.AreEqual(seqResult, flowResult)

        Check.QuickThrowOnFail(tester, maxRuns = __.FsCheckMaxNumberOfTests, shrink = false)


    [<Test>]
    member __.``2. CloudFlow : ofCloudFiles with custom deserializer`` () =
        let f(xs : string [][]) =
            let cfs = xs 
                     |> Array.map(fun text -> local { let! path = CloudPath.GetRandomFileName() in return! CloudFile.WriteAllLines(path, text) })
                     |> Cloud.Parallel
                     |> runOnCurrentProcess

            let paths = cfs |> Array.map (fun cf -> cf.Path)
            let x =     CloudFlow.OfCloudFiles(paths, (fun (stream : System.IO.Stream) -> seq { yield stream.Length })) 
                        |> CloudFlow.length
                        |> runOnCloud

            Assert.AreEqual(paths.Length, x)
        Check.QuickThrowOnFail(f, maxRuns = __.FsCheckMaxNumberOfIOBoundTests, shrink = false)

    [<Test>]
    member __.``2. CloudFlow : ofCloudFiles with small threshold`` () =
        let path = CloudPath.GetRandomFileName() |> runOnCurrentProcess
        let file = CloudFile.WriteAllText(path, "Lorem ipsum dolor sit amet") |> runOnCurrentProcess
        let f (size : int) =
            let size = abs size % 200
            let repeatedPaths = Array.init size (fun _ -> file.Path)
            let length =
                CloudFlow.OfCloudFiles(repeatedPaths, (fun (stream : System.IO.Stream) -> seq { yield stream.Length }), sizeThresholdPerCore = 1L)
                |> CloudFlow.length
                |> runOnCloud

            Assert.AreEqual(int64 size, length)
        Check.QuickThrowOnFail(f, maxRuns = __.FsCheckMaxNumberOfIOBoundTests, shrink = false)

    [<Test>]
    member __.``2. CloudFlow : ofCloudFiles with ReadLines`` () =
        let f(xs : string [][]) =
            let cfs = xs 
                     |> Array.map(fun text -> local { let! path = CloudPath.GetRandomFileName() in return! CloudFile.WriteAllLines(path, text) })
                     |> Cloud.Parallel
                     |> runOnCurrentProcess

            let x = cfs |> Array.map (fun cf -> cf.Path)
                        |> CloudFlow.OfCloudFileByLine
                        |> CloudFlow.toArray
                        |> runOnCloud
                        |> Set.ofArray
            
            let y = cfs |> Array.map (fun f -> __.RunLocally(cloud { return! CloudFile.ReadAllLines f.Path }))
                        |> Seq.collect id
                        |> Set.ofSeq

            Assert.AreEqual(y, x)
        Check.QuickThrowOnFail(f, maxRuns = __.FsCheckMaxNumberOfIOBoundTests, shrink = false)

    [<Test>]
    member __.``2. CloudFlow : ofCloudFilesByLine with ReadLines`` () =
        let f(xs : string [][]) =
            let cfs = xs 
                     |> Array.map(fun text -> local { let! path = CloudPath.GetRandomFileName() in return! CloudFile.WriteAllLines(path, text) })
                     |> Cloud.Parallel
                     |> runOnCurrentProcess

            let x = cfs |> Array.map (fun cf -> cf.Path)
                        |> CloudFlow.OfCloudFileByLine
                        |> CloudFlow.toArray
                        |> runOnCloud
                        |> Set.ofArray
            
            let y = cfs |> Array.map (fun f -> __.RunLocally(cloud { return! CloudFile.ReadAllLines f.Path }))
                        |> Seq.collect id
                        |> Set.ofSeq

            Assert.AreEqual(y, x)
        Check.QuickThrowOnFail(f, maxRuns = __.FsCheckMaxNumberOfIOBoundTests, shrink = false)
    
    
    [<Test>]
    member __.``2. CloudFlow : OfCloudFileByLine`` () =
        
        let f(xs : string [], separator : Separator) =
            let separator = 
                match separator with
                | N -> "\n" 
                | R -> "\r"
                | RN -> "\r\n"

            let path = CloudPath.GetRandomFileName() |> runOnCurrentProcess
            let cf = CloudFile.WriteAllText(path, xs |> String.concat separator) |> runOnCurrentProcess
            let path = cf.Path

            let x = 
                path 
                |> CloudFlow.OfCloudFileByLine
                |> CloudFlow.toArray
                |> runOnCloud
                |> Array.sortBy id
                    
            
            let y = 
                __.RunLocally(cloud { return! CloudFile.ReadLines cf.Path })
                |> Seq.sortBy id
                |> Seq.toArray
                    
            Assert.AreEqual(y, x)
        Check.QuickThrowOnFail(f, maxRuns = __.FsCheckMaxNumberOfIOBoundTests, shrink = false)


    [<Test>]
    member __.``2. CloudFlow : OfCloudFileByLine with Big TextFile `` () =
        
        let f(count : int, separator : Separator) =
            let separator = 
                match separator with
                | N -> "\n" 
                | R -> "\r"
                | RN -> "\r\n"

            let path = CloudPath.GetRandomFileName() |> runOnCurrentProcess
            let cf = CloudFile.WriteAllText(path, [|1..(Math.Abs(count) * 1000)|] |> Array.map string |> String.concat separator) |> runOnCurrentProcess
            let path = cf.Path

            let x = 
                path 
                |> CloudFlow.OfCloudFileByLine
                |> CloudFlow.length
                |> runOnCloud
                
            let y = 
                __.RunLocally(cloud { let! lines = CloudFile.ReadLines path in return lines |> Seq.length })
                            
            Assert.AreEqual(y, x)
        Check.QuickThrowOnFail(f, maxRuns = __.FsCheckMaxNumberOfIOBoundTests, shrink = false)

    [<Test>]
    member __.``2. CloudFlow : ofCloudFiles with ReadAllLines`` () =
        let f(xs : string [][]) =
            let cfs = xs 
                     |> Array.map(fun text -> local { let! path = CloudPath.GetRandomFileName() in return! CloudFile.WriteAllLines(path, text) })
                     |> Cloud.Parallel
                     |> runOnCurrentProcess

            let x = cfs 
                        |> Array.map (fun cf -> cf.Path)
                        |> CloudFlow.OfCloudFileByLine
                        |> CloudFlow.toArray
                        |> runOnCloud
                        |> Set.ofArray

            let y = cfs |> Array.map (fun f -> __.RunLocally(cloud { return! CloudFile.ReadAllLines f.Path }))
                        |> Seq.collect id
                        |> Set.ofSeq

            Assert.AreEqual(y, x)
        Check.QuickThrowOnFail(f, maxRuns = __.FsCheckMaxNumberOfIOBoundTests, shrink = false)

    [<Test>]
    member __.``2. CloudFlow : OfHttpFileByLine single input`` () =
        if not isCIInstance then
            for url in testUrls do
                let lineCount = getHttpFileLineCount url

                let flowLength = 
                    CloudFlow.OfHttpFileByLine url
                    |> CloudFlow.length
                    |> runOnCloud
                            
                Assert.AreEqual(lineCount, flowLength)

    [<Test>]
    member __.``2. CloudFlow : OfHttpFileByLine multiple inputs`` () =
        if not isCIInstance then
            let lineCount = testUrls |> Array.Parallel.map getHttpFileLineCount |> Array.sum

            let flowLength =
                CloudFlow.OfHttpFileByLine testUrls
                |> CloudFlow.length
                |> runOnCloud

            Assert.AreEqual(lineCount, flowLength)

    [<Test>]
    member __.``2. CloudFlow : map`` () =
        let f(xs : int[]) =
            let x = xs |> CloudFlow.OfArray |> CloudFlow.map (fun n -> 2 * n) |> CloudFlow.toArray |> runOnCloud
            let y = xs |> Seq.map (fun n -> 2 * n) |> Seq.toArray
            Assert.AreEqual(y, x)
        Check.QuickThrowOnFail(f, maxRuns = __.FsCheckMaxNumberOfTests, shrink = false)

    [<Test>]
    member __.``2. CloudFlow : peek`` () =
        let f(xs : int[]) =
            let wf = cloud {
                let! ys = xs |> CloudFlow.OfArray |> CloudFlow.peek (Cloud.Logf "%d") |> CloudFlow.toArray
                Assert.AreEqual(Array.sort ys, Array.sort xs)
            }
            let logs = __.RunWithLogs wf
            test <@ logs.Length = xs.Length @>

        Check.QuickThrowOnFail(f, maxRuns = __.FsCheckMaxNumberOfTests, shrink = false)

    [<Test>]
    member __.``2. CloudFlow : filter`` () =
        let f(xs : int[]) =
            let x = xs |> CloudFlow.OfArray |> CloudFlow.filter (fun n -> n % 2 = 0) |> CloudFlow.toArray |> runOnCloud
            let y = xs |> Seq.filter (fun n -> n % 2 = 0) |> Seq.toArray
            Assert.AreEqual(y, x)
        Check.QuickThrowOnFail(f, maxRuns = __.FsCheckMaxNumberOfTests, shrink = false)

    [<Test>]
    member __.``2. CloudFlow : choose`` () =
        let f(xs : int[]) =
            let x = xs |> CloudFlow.OfArray |> CloudFlow.choose (fun n -> if n % 2 = 0 then Some(string n) else None) |> CloudFlow.toArray |> runOnCloud
            let y = xs |> Seq.choose (fun n -> if n % 2 = 0 then Some(string n) else None) |> Seq.toArray
            Assert.AreEqual(y, x)
        Check.QuickThrowOnFail(f, maxRuns = __.FsCheckMaxNumberOfTests, shrink = false)

    [<Test>]
    member __.``2. CloudFlow : collect`` () =
        let f(xs : int[]) =
            let x = xs |> CloudFlow.OfArray |> CloudFlow.collect (fun n -> [|1..n|]) |> CloudFlow.toArray |> runOnCloud
            let y = xs |> Seq.collect (fun n -> [|1..n|]) |> Seq.toArray
            Assert.AreEqual(y, x)
        Check.QuickThrowOnFail(f, maxRuns = __.FsCheckMaxNumberOfTests, shrink = false)

    [<Test>]
    member __.``2. CloudFlow : fold`` () =
        let f(xs : int[]) =
            let x = xs |> CloudFlow.OfArray |> CloudFlow.map (fun n -> 2 * n) |> CloudFlow.fold (+) (+) (fun () -> 0) |> runOnCloud
            let y = xs |> Seq.map (fun n -> 2 * n) |> Seq.fold (+) 0 
            Assert.AreEqual(y, x)
        Check.QuickThrowOnFail(f, maxRuns = __.FsCheckMaxNumberOfTests, shrink = false)

    [<Test>]
    member __.``2. CloudFlow : sum`` () =
        let f(xs : int[]) =
            let x = xs |> CloudFlow.OfArray |> CloudFlow.map (fun n -> 2 * n) |> CloudFlow.sum |> runOnCloud
            let y = xs |> Seq.map (fun n -> 2 * n) |> Seq.sum
            Assert.AreEqual(y, x)
        Check.QuickThrowOnFail(f, maxRuns = __.FsCheckMaxNumberOfTests, shrink = false)

    [<Test>]
    member __.``2. CloudFlow : length`` () =
        let f(xs : int[]) =
            let x = xs |> CloudFlow.OfArray |> CloudFlow.filter (fun n -> n % 2 = 0) |> CloudFlow.length |> runOnCloud
            let y = xs |> Seq.filter (fun n -> n % 2 = 0) |> Seq.length
            Assert.AreEqual(y, int x)
        Check.QuickThrowOnFail(f, maxRuns = __.FsCheckMaxNumberOfTests, shrink = false)


    [<Test>]
    member __.``2. CloudFlow : countBy`` () =
        let f(xs : int[]) =
            let x = xs |> CloudFlow.OfArray |> CloudFlow.countBy id |> CloudFlow.toArray |> runOnCloud
            let y = xs |> Seq.countBy id |> Seq.map (fun (k,c) -> k, int64 c) |> Seq.toArray
            Assert.AreEqual(set y, set x)
        Check.QuickThrowOnFail(f, maxRuns = __.FsCheckMaxNumberOfTests, shrink = false)


    [<Test>]
    member __.``2. CloudFlow : sortBy`` () =
        let f(xs : int[]) =
            let x = xs |> CloudFlow.OfArray |> CloudFlow.sortBy id 10 |> CloudFlow.toArray |> runOnCloud
            let y = (xs |> Seq.sortBy id).Take(10).ToArray()
            Assert.AreEqual(y, x)
        Check.QuickThrowOnFail(f, maxRuns = __.FsCheckMaxNumberOfTests, shrink = false)

    [<Test>]
    member __.``2. CloudFlow : maxBy`` () =
        let f(xs : int[]) =
            if Array.isEmpty xs then
                try let _ = xs |> CloudFlow.OfArray |> CloudFlow.maxBy id |> runOnCloud in false
                with :? System.ArgumentException -> true
            else
                let x = xs |> CloudFlow.OfArray |> CloudFlow.maxBy id  |> runOnCloud
                let y = xs |> Seq.maxBy id
                x = y

        Check.QuickThrowOnFail(f, maxRuns = __.FsCheckMaxNumberOfTests, shrink = false)

    [<Test>]
    member __.``2. CloudFlow : minBy`` () =
        let f(xs : int[]) =
            if Array.isEmpty xs then
                try let _ = xs |> CloudFlow.OfArray |> CloudFlow.minBy id |> runOnCloud in false
                with :? System.ArgumentException -> true
            else
                let x = xs |> CloudFlow.OfArray |> CloudFlow.minBy id  |> runOnCloud
                let y = xs |> Seq.minBy id
                x = y

        Check.QuickThrowOnFail(f, maxRuns = __.FsCheckMaxNumberOfTests, shrink = false)

    [<Test>]
    member __.``2. CloudFlow : reduce`` () =
        let f(xs : int[]) =
            if Array.isEmpty xs then
                try let _ = xs |> CloudFlow.OfArray |> CloudFlow.reduce (+) |> runOnCloud in false
                with :? System.ArgumentException -> true
            else
                let x = xs |> CloudFlow.OfArray |> CloudFlow.reduce (+) |> runOnCloud
                let y = xs |> Seq.reduce (+)
                x = y

        Check.QuickThrowOnFail(f, maxRuns = __.FsCheckMaxNumberOfTests, shrink = false)

    [<Test>]
    member __.``2. CloudFlow : reduceBy`` () =
        let f(xs : int []) =
            let x = xs |> CloudFlow.OfArray |> CloudFlow.reduceBy id (+) |> CloudFlow.toArray |> runOnCloud
            let y = xs |> Seq.groupBy id |> Seq.map (fun (k, vs) -> k, vs |> Seq.reduce (+)) |> Seq.toArray
            Assert.AreEqual(Array.sortBy fst y, Array.sortBy fst x)

        Check.QuickThrowOnFail(f, maxRuns = __.FsCheckMaxNumberOfTests, shrink = false)

    [<Test>]
    member __.``2. CloudFlow : averageBy`` () =
        let f(xs : int[]) =
            if Array.isEmpty xs then
                try let _ = xs |> CloudFlow.OfArray |> CloudFlow.averageBy (float) |> runOnCloud in false
                with :? System.ArgumentException -> true
            else
                let x = xs |> CloudFlow.OfArray |> CloudFlow.averageBy (float) |> runOnCloud
                let y = xs |> Seq.averageBy (float)
                x = y

        Check.QuickThrowOnFail(f, maxRuns = __.FsCheckMaxNumberOfTests, shrink = false)

    [<Test>]
    member __.``2. CloudFlow : groupBy`` () =
        let f(xs : int[]) =
            let x =
                xs
                |> CloudFlow.OfArray
                |> CloudFlow.groupBy id
                |> CloudFlow.map (fun (k, xs) -> k, Seq.length xs)
                |> CloudFlow.toArray
                |> runOnCloud
            let y =
                xs
                |> Seq.groupBy id
                |> Seq.map (fun (k, xs) -> k, Seq.length xs)
                |> Seq.toArray
            (x |> Array.sortBy fst) = (y |> Array.sortBy fst)

        Check.QuickThrowOnFail(f, maxRuns = __.FsCheckMaxNumberOfTests, shrink = false)

    [<Test>]
    member __.``2. CloudFlow : averageByKey`` () =
        let f(xs : int[]) =
            let x =
                xs
                |> CloudFlow.OfArray
                |> CloudFlow.averageByKey id float
                |> CloudFlow.toArray
                |> runOnCloud
            let y =
                xs
                |> Seq.groupBy id
                |> Seq.map (fun (k, xs) -> k, Seq.averageBy float xs)
                |> Seq.toArray
            (x |> Array.sortBy fst |> Array.map (fun (k,v) -> (k,int (v * 10.0)))) = (y |> Array.sortBy fst |> Array.map (fun (k,v) -> (k,int (v * 10.0))))

        Check.QuickThrowOnFail(f, maxRuns = __.FsCheckMaxNumberOfTests, shrink = false)

    [<Test>]
    member __.``2. CloudFlow : sumByKey`` () =
        let f(xs : int[]) =
            let x =
                xs
                |> CloudFlow.OfArray
                |> CloudFlow.sumByKey id id
                |> CloudFlow.toArray
                |> runOnCloud
            let y =
                xs
                |> Seq.groupBy id
                |> Seq.map (fun (k, xs) -> k, Seq.sumBy id xs)
                |> Seq.toArray
            (x |> Array.sortBy fst) = (y |> Array.sortBy fst)

        Check.QuickThrowOnFail(f, maxRuns = __.FsCheckMaxNumberOfTests, shrink = false)

    [<Test>]
    member __.``2. CloudFlow : groupJoinBy`` () =
        let f(xs : int[], ys : int[]) =
            let x =
                xs
                |> CloudFlow.OfArray
                |> CloudFlow.groupJoinBy id id (CloudFlow.OfArray ys)
                |> CloudFlow.collect (fun (_, xs, ys) -> xs |> Seq.collect (fun x -> ys |> Seq.map (fun y -> (x, y))))
                |> CloudFlow.toArray
                |> runOnCloud
            let y =
                xs.Join(ys, (fun x -> x), (fun y -> y), (fun x y -> (x, y))).ToArray()
                
            (x |> Array.sortBy id) = (y |> Array.sortBy id)

        Check.QuickThrowOnFail(f, maxRuns = __.FsCheckMaxNumberOfTests, shrink = false)


    [<Test>]
    member __.``2. CloudFlow : join`` () =
        let f(xs : int[], ys : int[]) =
            let x =
                xs
                |> CloudFlow.OfArray
                |> CloudFlow.join id id (CloudFlow.OfArray ys)
                |> CloudFlow.map (fun (_, x, y) -> (x, y))
                |> CloudFlow.toArray
                |> runOnCloud
            let y =
                xs.Join(ys, (fun x -> x), (fun y -> y), (fun x y -> (x, y))).ToArray()
                
            (x |> Array.sortBy id) = (y |> Array.sortBy id)

        Check.QuickThrowOnFail(f, maxRuns = __.FsCheckMaxNumberOfTests, shrink = false)


    [<Test>]
    member __.``2. CloudFlow : leftOuterJoin`` () =
        let f(xs : int[], ys : int[]) =
            let x =
                xs
                |> CloudFlow.OfArray
                |> CloudFlow.leftOuterJoin id id (CloudFlow.OfArray ys)
                |> CloudFlow.map (fun (_, x, y) -> (x, y))
                |> CloudFlow.toArray
                |> runOnCloud
            let y =
                xs.GroupJoin(ys, (fun x -> x), (fun y -> y), (fun x ys -> if Seq.isEmpty ys then Seq.singleton (x, None) else ys |> Seq.map (fun y -> (x, Some y)))).SelectMany(fun x -> x).ToArray()
                
            (x |> Array.sortBy id) = (y |> Array.sortBy id)

        Check.QuickThrowOnFail(f, maxRuns = __.FsCheckMaxNumberOfTests, shrink = false)


    [<Test>]
    member __.``2. CloudFlow : rightOuterJoin`` () =
        let f(xs : int[], ys : int[]) =
            let x =
                xs
                |> CloudFlow.OfArray
                |> CloudFlow.rightOuterJoin id id (CloudFlow.OfArray ys)
                |> CloudFlow.map (fun (_, x, y) -> (x, y))
                |> CloudFlow.toArray
                |> runOnCloud
            let y =
                ys.GroupJoin(xs, (fun y -> y), (fun x -> x),  (fun y xs -> if Seq.isEmpty xs then Seq.singleton (None, y) else xs |> Seq.map (fun x -> (Some x, y)))).SelectMany(fun x -> x).ToArray()
                
            (x |> Array.sortBy id) = (y |> Array.sortBy id)

        Check.QuickThrowOnFail(f, maxRuns = __.FsCheckMaxNumberOfTests, shrink = false)


    [<Test>]
    member __.``2. CloudFlow : fullOuterJoin`` () =
        let f(xs : int[], ys : int[]) =
            let x =
                xs
                |> CloudFlow.OfArray
                |> CloudFlow.fullOuterJoin id id (CloudFlow.OfArray ys)
                |> CloudFlow.map (fun (_, x, y) -> (x, y))
                |> CloudFlow.toArray
                |> runOnCloud
            let left =
                xs.GroupJoin(ys, (fun x -> x), (fun y -> y), (fun x ys -> if Seq.isEmpty ys then Seq.singleton (Some x, None) else ys |> Seq.map (fun y -> (Some x, Some y)))).SelectMany(fun x -> x).ToArray()
            let right =
                ys.GroupJoin(xs, (fun y -> y), (fun x -> x),  (fun y xs -> if Seq.isEmpty xs then Seq.singleton (None, Some y) else xs |> Seq.map (fun x -> (Some x, Some y)))).SelectMany(fun x -> x).ToArray()
            let full = left.Union(right).ToArray()

                
            (x |> set) = (full |> set)

        Check.QuickThrowOnFail(f, maxRuns = __.FsCheckMaxNumberOfTests, shrink = false)

    [<Test>]
    member __.``2. CloudFlow : distinctBy`` () =
        let f(xs : int[]) =
            let x =
                xs
                |> CloudFlow.OfArray
                |> CloudFlow.map (fun v -> Math.Abs v)
                |> CloudFlow.distinctBy id
                |> CloudFlow.toArray
                |> runOnCloud
            let y =
                xs
                |> Seq.map (fun v -> Math.Abs v)
                |> Seq.distinctBy id
                |> Seq.toArray
            (x |> Array.sortBy id) = (y |> Array.sortBy id)

        Check.QuickThrowOnFail(f, maxRuns = __.FsCheckMaxNumberOfTests, shrink = false)

    [<Test>]
    member __.``2. CloudFlow : distinct`` () =
        let f(xs : int[]) =
            let x =
                xs
                |> CloudFlow.OfArray
                |> CloudFlow.map (fun v -> Math.Abs v)
                |> CloudFlow.distinctBy id
                |> CloudFlow.toArray
                |> runOnCloud
            let y =
                xs
                |> Seq.map (fun v -> Math.Abs v)
                |> Seq.distinctBy id
                |> Seq.toArray
            (x |> Array.sortBy id) = (y |> Array.sortBy id)

        Check.QuickThrowOnFail(f, maxRuns = __.FsCheckMaxNumberOfTests, shrink = false)

    [<Test>]
    member __.``2. CloudFlow : take`` () =
        let f (xs : int[], n : int) =
            let n = System.Math.Abs(n)
            let x = xs |> CloudFlow.OfArray |> CloudFlow.take n |> CloudFlow.toArray |> runOnCloud
            Assert.AreEqual(min xs.Length n, x.Length)
        Check.QuickThrowOnFail(f, maxRuns = __.FsCheckMaxNumberOfTests, shrink = false)

    [<Test>]
    member __.``2. CloudFlow : withDegreeOfParallelism`` () =
        let f(xs : int[]) = 
            let r = xs 
                    |> CloudFlow.OfArray
                    |> CloudFlow.map (fun _ -> System.Diagnostics.Process.GetCurrentProcess().Id)
                    |> CloudFlow.withDegreeOfParallelism 1
                    |> CloudFlow.toArray
                    |> runOnCloud
            let x = r
                    |> Set.ofArray
                    |> Seq.length
            if xs.Length = 0 then x = 0
            else x = 1
        Check.QuickThrowOnFail(f, maxRuns = __.FsCheckMaxNumberOfTests, shrink = false)

    [<Test>]
    member __.``2. CloudFlow : verify multicore utilization`` () = 
        let f(count : int) = 
            let path = CloudPath.GetRandomFileName() |> runOnCurrentProcess
            let cf = CloudFile.WriteAllLines(path, [|1.. ((abs count + 10) * 1000)|] |> Array.map string) |> runOnCurrentProcess
            let r = cf.Path
                    |> CloudFlow.OfCloudFileByLine
                    |> CloudFlow.map (fun _ -> Thread.CurrentThread.ManagedThreadId)
                    |> CloudFlow.toArray
                    |> runOnCurrentProcess
                    |> Seq.distinct 
                    |> Seq.length
            r > 1
            
        try Check.QuickThrowOnFail(f, maxRuns = __.FsCheckMaxNumberOfIOBoundTests, shrink = false)
        with e -> Assert.Inconclusive(sprintf "Test failed with %O" e)

    [<Test>]
    member __.``2. CloudFlow : tryFind`` () =
        let f(xs : int[]) =
            let x = xs |> CloudFlow.OfArray |> CloudFlow.tryFind (fun n -> n = 0) |> runOnCloud
            let y = xs |> Seq.tryFind (fun n -> n = 0) 
            x = y
        Check.QuickThrowOnFail(f, maxRuns = __.FsCheckMaxNumberOfTests, shrink = false)

    [<Test>]
    member __.``2. CloudFlow : find`` () =
        let f(xs : int[]) =
            let x = try xs |> CloudFlow.OfArray |> CloudFlow.find (fun n -> n = 0) |> runOnCloud with | :? KeyNotFoundException -> -1
            let y = try xs |> Seq.find (fun n -> n = 0) with | :? KeyNotFoundException -> -1
            x = y
        Check.QuickThrowOnFail(f, maxRuns = __.FsCheckMaxNumberOfTests, shrink = false)

    [<Test>]
    member __.``2. CloudFlow : tryPick`` () =
        let f(xs : int[]) =
            let x = xs |> CloudFlow.OfArray |> CloudFlow.tryPick (fun n -> if n = 0 then Some n else None) |> runOnCloud
            let y = xs |> Seq.tryPick (fun n -> if n = 0 then Some n else None) 
            x = y
        Check.QuickThrowOnFail(f, maxRuns = __.FsCheckMaxNumberOfTests, shrink = false)

    [<Test>]
    member __.``2. CloudFlow : pick`` () =
        let f(xs : int[]) =
            let x = try xs |> CloudFlow.OfArray |> CloudFlow.pick (fun n -> if n = 0 then Some n else None) |> runOnCloud with | :? KeyNotFoundException -> -1
            let y = try xs |> Seq.pick (fun n -> if n = 0 then Some n else None)  with | :? KeyNotFoundException -> -1
            x = y
        Check.QuickThrowOnFail(f, maxRuns = __.FsCheckMaxNumberOfTests, shrink = false)

    [<Test>]
    member __.``2. CloudFlow : exists`` () =
        let f(xs : int[]) =
            let x = xs |> CloudFlow.OfArray |> CloudFlow.exists (fun n -> n = 0) |> runOnCloud
            let y = xs |> Seq.exists (fun n -> n = 0) 
            x = y
        Check.QuickThrowOnFail(f, maxRuns = __.FsCheckMaxNumberOfTests, shrink = false)


    [<Test>]
    member __.``2. CloudFlow : forall`` () =
        let f(xs : int[]) =
            let x = xs |> CloudFlow.OfArray |> CloudFlow.forall (fun n -> n = 0) |> runOnCloud
            let y = xs |> Seq.forall (fun n -> n = 0) 
            x = y
        Check.QuickThrowOnFail(f, maxRuns = __.FsCheckMaxNumberOfTests, shrink = false)

    [<Test>]
    member __.``2. CloudFlow : isEmpty`` () =
        let f(xs : int[]) =
            let x = xs |> CloudFlow.OfArray |> CloudFlow.isEmpty |> runOnCloud
            let y = xs |> Seq.isEmpty
            x = y

        Check.QuickThrowOnFail(f, maxRuns = __.FsCheckMaxNumberOfTests, shrink = false)

    [<Test>]
    member __.``2. CloudFlow : ofCloudQueue`` () =
        let f(_ : int) =
            let x =
                cloud {
                    let! queue = CloudQueue.New()
                    let! n =
                        Cloud.Choice [
                            cloud { 
                                for i in [|1..1000|] do
                                    do! queue.EnqueueAsync i
                                    do! Cloud.Sleep(100)
                                return None
                            };
                            cloud {
                                let! n =  
                                    CloudFlow.OfCloudQueue(queue, 1)
                                    |> CloudFlow.take 2
                                    |> CloudFlow.length
                                return Some n
                            }]
                    return Option.get n
                }
                |> runOnCloud
            x = 2L
        Check.QuickThrowOnFail(f, maxRuns = __.FsCheckMaxNumberOfIOBoundTests, shrink = false)


    [<Test>]
    member __.``2. CloudFlow : toCloudQueue`` () =
        let f(xs : int[]) =
            let queue = CloudQueue.New() |> runOnCloud
            let _ = 
                xs
                |> CloudFlow.OfArray
                |> CloudFlow.map (fun v -> v + 1)
                |> CloudFlow.toCloudQueue queue
                |> runOnCloud

            let x = 
                cloud {
                    let list = ResizeArray<int>()
                    for _ in xs do 
                        let! v = queue.DequeueAsync()
                        list.Add(v)
                    return list
                } |> runOnCloud
            let y = xs |> Seq.map (fun v -> v + 1) |> Seq.toArray
            (set x) = (set y)
        Check.QuickThrowOnFail(f, maxRuns = __.FsCheckMaxNumberOfIOBoundTests, shrink = false)


    [<Test>]
    member __.``2. CloudFlow : toTextCloudFiles`` () =
        let f(xs : int[]) =
            let xs = xs |> Array.map string
            let dir = CloudPath.GetRandomDirectoryName() |> __.RunLocally
            let cfs = 
                xs
                |> CloudFlow.OfArray
                |> CloudFlow.toTextCloudFiles dir
                |> runOnCloud
            let ys = cfs |> Array.map (fun f -> __.RunLocally(cloud { return! CloudFile.ReadAllLines f.Path }))
                        |> Seq.collect id
                        |> Seq.toArray
            Assert.AreEqual(xs, ys)
        Check.QuickThrowOnFail(f, maxRuns = __.FsCheckMaxNumberOfIOBoundTests, shrink = false)
