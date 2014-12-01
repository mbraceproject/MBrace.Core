namespace Nessos.MBrace.Store.Tests.Azure

open NUnit.Framework
open FsUnit

open Nessos.MBrace
open Nessos.MBrace.Store
open Nessos.MBrace.Azure.Store
open Nessos.MBrace.Store.Tests


module Helper =
    open System
    let conn = Environment.GetEnvironmentVariable("azurestorageconn", EnvironmentVariableTarget.User)
    let store = lazy new BlobStore(conn)

[<TestFixture>]
type ``Azure Blob store tests`` () =
    inherit  ``File Store Tests``(Helper.store.Value)

    static do
        StoreRegistry.Register(Helper.store.Value)

//[<TestFixture>]
//type ``FileSystem Table store tests`` () =
//    inherit  ``Table Store Tests``(StoreConfiguration.fileSystemStore)
//
//[<TestFixture>]
//type ``FileSystem MBrace tests`` () =
//    inherit ``Local MBrace store tests``(StoreConfiguration.fileSystemStore, StoreConfiguration.fileSystemStore)