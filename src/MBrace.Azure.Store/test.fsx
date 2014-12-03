// Learn more about F# at http://fsharp.net. See the 'F# Tutorial' project
// for more guidance on F# programming.

#I "../../bin/"
#r "MBrace.Azure.Store"
#r "MBrace.Core"

open System
open Nessos.MBrace.Store
open Nessos.MBrace.Azure.Store
open Nessos.MBrace

// Define your library scripting code here

let conn = Environment.GetEnvironmentVariable("azurestorageconn", EnvironmentVariableTarget.User)

let fileStore = new BlobStore(conn) :> ICloudFileStore
let tableStore = new TableStore(conn) :> ICloudTableStore

let run = Async.RunSynchronously

let testContainer = fileStore.CreateUniqueContainerName()


let container = fileStore.CreateUniqueContainerName()
fileStore.ContainerExists container |> run //|> should equal false
fileStore.CreateContainer container |> run
fileStore.ContainerExists container |> run //|> should equal true
fileStore.DeleteContainer container |> run
fileStore.ContainerExists container |> run //|> should equal false

let data = Array.init (1024 * 1024 * 4) byte
let file = fileStore.CreateUniqueFileName testContainer
do
    use stream = fileStore.BeginWrite file |> run
    stream.Write(data, 0, data.Length)

do
    use m = new MemoryStream()
    use stream = fileStore.BeginRead file |> run
    stream.CopyTo m
    m.ToArray() |> should equal data
        
fileStore.DeleteFile file |> run





let file = fileStore.CreateUniqueFileName testContainer

fileStore.FileExists file |> run |> should equal false

// write to file
do
    use stream = fileStore.BeginWrite file |> run
    for i = 1 to 100 do stream.WriteByte(byte i)

fileStore.FileExists file |> run |> should equal true
fileStore.EnumerateFiles testContainer |> run |> Array.exists ((=) file) |> should equal true

// read from file
do
    use stream = fileStore.BeginRead file |> run
    for i = 1 to 100 do
        stream.ReadByte() |> should equal i

fileStore.DeleteFile file |> run

fileStore.FileExists file |> run |> should equal false

StoreRegistry.Register(fileStore)
StoreRegistry.Register(tableStore)

type ICounter =
    abstract GetValue : unit -> Async<int>
    abstract Increase : int  -> Async<unit>

type NaiveCounter (atom, tableStore : ICloudTableStore) =
    interface ICounter with
        member __.Increase _ =  tableStore.Update(atom, fun v -> v + 1)
        member __.GetValue() = tableStore.GetValue<int>(atom)
    static member Create(initial, tableStore : ICloudTableStore) =
        async {
            let! atom = tableStore.Create initial
            return NaiveCounter(atom, tableStore)
        } |> Async.RunSynchronously


type ScalingRandomCounter (atoms : string [], tableStore : ICloudTableStore) =
    let atom = 
        let rng = new Random(int DateTime.Now.Ticks)
        fun () -> atoms.[rng.Next(0, atoms.Length)]

    interface ICounter with
        member __.Increase _ = tableStore.Update(atom(), fun v -> v + 1)
        member __.GetValue() = 
            async {
                let! xs =
                    atoms |> Seq.map (fun atom -> tableStore.GetValue<int>(atom))
                          |> Async.Parallel
                return Array.sum xs
            }
    static member Create(initial, scale, tableStore : ICloudTableStore) =
        async {
            let! atoms = Array.init scale (fun _ -> tableStore.Create 0)
                         |> Async.Parallel
            do! tableStore.Update(atoms.[0], fun _ -> initial)
            return ScalingRandomCounter(atoms, tableStore)
        } |> Async.RunSynchronously

type ScalingAffinityCounter (atoms : string [], tableStore : ICloudTableStore) =
    interface ICounter with
        member __.Increase id =  tableStore.Update(atoms.[id], fun v -> v + 1)
        member __.GetValue() = 
            async {
                let! xs =
                    atoms |> Seq.map (fun atom -> tableStore.GetValue<int>(atom))
                          |> Async.Parallel
                return Array.sum xs
            }
    static member Create(initial, scale, tableStore : ICloudTableStore) =
        async {
            let! atoms = Array.init scale (fun _ -> tableStore.Create 0)
                         |> Async.Parallel
            do! tableStore.Update(atoms.[0], fun _ -> initial)
            return ScalingRandomCounter(atoms, tableStore)
        } |> Async.RunSynchronously


let test1 npar nseq (cnt : ICounter) =
    async {
        do! [1..npar]
            |> Seq.map (fun i ->
                async {
                    for j = 1 to nseq do
                        do! cnt.Increase i
                })
            |> Async.Parallel
            |> Async.Ignore
        return! cnt.GetValue()
    }

let test2 npar nseq (cnt : ICounter) =
    async {
        do! [1..npar]
            |> Seq.map (fun i ->
                async {
                    for j = 1 to nseq do
                        do! cnt.Increase i
                        let _ = cnt.GetValue()
                        ()
                })
            |> Async.Parallel
            |> Async.Ignore
        return! cnt.GetValue()
    }


#time "on"

//Real: 00:01:58.067, CPU: 00:00:03.650, GC gen0: 84, gen1: 34, gen2: 0
NaiveCounter.Create(0, tableStore) 
    |> test1 10 15 
    |> Async.RunSynchronously 
//Real: 00:00:47.729, CPU: 00:00:00.780, GC gen0: 34, gen1: 27, gen2: 1
ScalingRandomCounter.Create(0, 10, tableStore) 
    |> test1 10 15 
    |> Async.RunSynchronously 

//Real: 00:00:49.195, CPU: 00:00:00.904, GC gen0: 37, gen1: 32, gen2: 0
ScalingAffinityCounter.Create(0, 10, tableStore) 
    |> test1 10 15 
    |> Async.RunSynchronously 

//Real: 00:02:17.372, CPU: 00:00:01.872, GC gen0: 88, gen1: 47, gen2: 1
NaiveCounter.Create(0, tableStore) 
    |> test2 10 15 
    |> Async.RunSynchronously 
//Real: 00:00:50.039, CPU: 00:00:00.889, GC gen0: 37, gen1: 34, gen2: 0
ScalingRandomCounter.Create(0, 10, tableStore) 
    |> test2 10 15 
    |> Async.RunSynchronously 
//Real: 00:00:48.924, CPU: 00:00:00.858, GC gen0: 39, gen1: 15, gen2: 1
ScalingAffinityCounter.Create(0, 10, tableStore) 
    |> test2 10 15 
    |> Async.RunSynchronously 




#r "Microsoft.WindowsAzure.Storage"
#r "Microsoft.WindowsAzure.Configuration"

open Microsoft.WindowsAzure.Storage.Table
open Microsoft.WindowsAzure.Storage


type TupleEntity<'T> (pk, rk, value : 'T) =
    inherit TableEntity(pk, rk)
    member val Item1 = value with get, set
    new () = TupleEntity<'T>(null, null, Unchecked.defaultof<'T>)

type TupleEntity<'T,'U> (pk, rk, v1 : 'T, v2 : 'U) =
    inherit TableEntity(pk, rk)
    member val Item1 = v1 with get, set
    member val Item2 = v2 with get, set
    new () = TupleEntity<'T,'U>(null, null, Unchecked.defaultof<'T>, Unchecked.defaultof<'U>)



let acc = CloudStorageAccount.Parse(conn)
let client = acc.CreateCloudTableClient()

let table = client.GetTableReference("temp")
table.CreateIfNotExists()
let guid () = Guid.NewGuid().ToString("N")

let e = new FatEntity(guid(), guid(), [|42uy|])
let insert = TableOperation.Insert(e)
let r = table.Execute(insert)
r.Result


let e = new TupleEntity<int>(guid (), guid(), 42)
let insert = TableOperation.Insert(e)
let r = table.Execute(insert)
r.Result


let e = new TupleEntity<_,_>(guid (), guid(), 42, DateTime.Now)
let insert = TableOperation.Insert(e)
let r = table.Execute(insert)
r.Result


generate 20
