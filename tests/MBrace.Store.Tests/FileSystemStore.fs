namespace Nessos.MBrace.Store.Tests

open NUnit.Framework
open FsUnit

[<TestFixture>]
type ``FileSystem File store tests`` () =
    inherit  ``File Store Tests``(StoreConfiguration.fileSystemStore)