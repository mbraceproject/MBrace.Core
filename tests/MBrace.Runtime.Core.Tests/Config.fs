namespace MBrace.Runtime.Tests

open MBrace.Core.Internals

open MBrace.Runtime
open MBrace.Runtime.Store

[<AutoOpen>]
module private Config =
    do VagabondRegistry.Initialize(throwOnError = false)

    let _ = System.Threading.ThreadPool.SetMinThreads(100, 100)

    let fsStore = FileSystemStore.CreateSharedLocal()
    let serializer = new FsPicklerBinaryStoreSerializer()
    let fsConfig = CloudFileStoreConfiguration.Create(fsStore)