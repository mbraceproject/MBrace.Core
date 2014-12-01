// Learn more about F# at http://fsharp.net. See the 'F# Tutorial' project
// for more guidance on F# programming.

#I "../../bin/"
#r "MBrace.Azure.Store"
#r "MBrace.Core"

open System
open Nessos.MBrace.Store
open Nessos.MBrace.Azure.Store

// Define your library scripting code here

let conn = Environment.GetEnvironmentVariable("azurestorageconn", EnvironmentVariableTarget.User)

let fileStore = new BlobStore(conn) :> ICloudFileStore

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