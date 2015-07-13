namespace MBrace.Runtime.Tests

open MBrace.Core
open MBrace.Core.Internals
open MBrace.Runtime.Vagabond
open MBrace.Runtime.Store
open MBrace.Runtime.InMemoryRuntime

#nowarn "044"

[<AutoOpen>]
module private Config =
    do VagabondRegistry.Initialize(throwOnError = false)

    let _ = System.Threading.ThreadPool.SetMinThreads(100, 100)

    let fsStore = FileSystemStore.CreateSharedLocal()
    let serializer = new FsPicklerBinaryStoreSerializer()
    let fsConfig = CloudFileStoreConfiguration.Create(fsStore)