namespace Nessos.MBrace.Store.Tests.Azure

open NUnit.Framework
open FsUnit

open Nessos.MBrace
open Nessos.MBrace.Store
open Nessos.MBrace.Azure.Store
open Nessos.MBrace.Store.Tests

module Helper =
    open System

    let selectEnv name =
        (Environment.GetEnvironmentVariable(name,EnvironmentVariableTarget.User),
            Environment.GetEnvironmentVariable(name,EnvironmentVariableTarget.Machine),
            Environment.GetEnvironmentVariable(name,EnvironmentVariableTarget.Process))
        |> function 
            | s, _, _ when not <| String.IsNullOrEmpty(s) -> s
            | _, s, _ when not <| String.IsNullOrEmpty(s) -> s
            | _, _, s when not <| String.IsNullOrEmpty(s) -> s
            | _ -> failwith "Variable not found"

    let conn = selectEnv "azurestorageconn"
    let blobStore = lazy new BlobStore(conn)
    let tableStore = lazy new TableStore(conn, Nessos.MBrace.Runtime.VagrantRegistry.Pickler)

[<TestFixture>]
type ``Azure Blob store tests`` () =
    inherit  ``File Store Tests``(Helper.blobStore.Value)

    static do
        StoreRegistry.TryRegister(Helper.blobStore.Value) |> ignore

[<TestFixture>]
type ``Azure Table store tests`` () =
    inherit  ``Table Store Tests``(Helper.tableStore.Value)

    static do
        StoreRegistry.TryRegister(Helper.tableStore.Value) |> ignore

[<TestFixture>]
type ``Azure MBrace tests`` () =
    inherit ``Local MBrace store tests``(Helper.blobStore.Value, Helper.tableStore.Value)

    static do
        StoreRegistry.TryRegister(Helper.blobStore.Value) |> ignore
        StoreRegistry.TryRegister(Helper.tableStore.Value) |> ignore
