namespace Nessos.MBrace.Store.Tests

open System

open Nessos.MBrace
open Nessos.MBrace.Continuation
open Nessos.MBrace.Store
open Nessos.MBrace.Runtime
open Nessos.MBrace.Runtime.Serialization
open Nessos.MBrace.Runtime.Store
open Nessos.MBrace.Tests

module StoreConfiguration =

    do VagrantRegistry.Initialize()

    let fileSystemStore = FileSystemStore.LocalTemp
    let serializer = FsPicklerStoreSerializer.Default
    do StoreRegistry.Register serializer

    let mkExecutionContext fileStore tableStoreOpt =
        let config =
            {
                FileStore = fileStore
                DefaultFileContainer = fileStore.CreateUniqueContainerName()
                TableStore = tableStoreOpt
                Serializer = serializer
            }

        resource { yield! InMemoryRuntime.Resource ; yield config }