[![Join the chat at https://gitter.im/mbraceproject/MBrace.Core](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/mbraceproject/MBrace.Core?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![NuGet Status](http://img.shields.io/nuget/vpre/MBrace.Core.svg?style=flat)](https://www.nuget.org/packages/MBrace.Core/)

# MBrace Core Libraries

This repository contains core libraries and runtime foundation components 
for the [MBrace](http://m-brace.net/) distributed computation programming model and frameworks.

## Repo Contents

### MBrace.Core [![NuGet Status](http://img.shields.io/nuget/vpre/MBrace.Core.svg?style=flat)](https://www.nuget.org/packages/MBrace.Core/)

[MBrace.Core](https://github.com/mbraceproject/MBrace.Core/tree/master/src/MBrace.Core) is a standalone class library that contains the core MBrace programming model, used to author general-purpose, runtime-agnostic distributed computation. It is centered on the concept of *cloud workflows*, a composable, language-integrated API based on F# computation expressions. It can be used to author specialized cloud libraries like MBrace.Flow.

### MBrace.Flow [![NuGet Status](http://img.shields.io/nuget/vpre/MBrace.Flow.svg?style=flat)](https://www.nuget.org/packages/MBrace.Flow/)

[MBrace.Flow](https://github.com/mbraceproject/MBrace.Core/tree/master/src/MBrace.Flow) is a distributed streaming library written on top of MBrace.Core. It enables distributed computation using functional pipeline declarations like the following:
```fsharp
CloudFlow.OfHttpFileByLine "http://my.server.local/large.txt"
|> CloudFlow.collect (fun line -> line.Split [|' '; ',' ; '.'|])
|> CloudFlow.filter (fun line -> line.Length > 3)
|> CloudFlow.countBy id
|> CloudFlow.take 50
```
It is written on top of the [Nessos.Streams](http://nessos.github.io/Streams) library, a fast streaming library inspired by [Java 8 Streams](http://www.oracle.com/technetwork/articles/java/ma14-java-se-8-streams-2177646.html).

### MBrace.CSharp

[MBrace.CSharp](https://github.com/mbraceproject/MBrace.Core/tree/master/src/MBrace.CSharp) contains C# friendly wrapper APIs for the MBrace core programming model. Development currently suspended.

### MBrace.Runtime [![NuGet Status](http://img.shields.io/nuget/vpre/MBrace.Runtime.svg?style=flat)](https://www.nuget.org/packages/MBrace.Runtime/)

[MBrace.Runtime](https://github.com/mbraceproject/MBrace.Core/tree/master/src/MBrace.Runtime) provides an extensive set of foundations and common components for quickly developing MBrace runtimes on top of [FsPickler](http://nessos.github.io/FsPickler)/[Vagabond](http://nessos.github.io/Vagabond). It removes the hassle of working with MBrace semantics and lets the runtime developer focus on providing cloud infrastructure implementations such as:
  * Cloud file storage.
  * Cloud table storage.
  * Work item queue/scheduler.
Refer to [MBrace.Thespian](https://github.com/mbraceproject/MBrace.Core/tree/master/samples/MBrace.Thespian) and [MBrace.Azure](https://github.com/mbraceproject/MBrace.Azure) for examples of MBrace runtimes that are implemented on top of MBrace.Runtime.

### MBrace.Tests [![NuGet Status](http://img.shields.io/nuget/vpre/MBrace.Tests.svg?style=flat)](https://www.nuget.org/packages/MBrace.Tests/)

[MBrace.Tests](https://github.com/mbraceproject/MBrace.Core/tree/master/tests/MBrace.Core.Tests) defines a comprehensive suite of abstracted NUnit/FsCheck based tests for use by MBrace runtime implementations. Useful for verifying that an MBrace implementation is up to spec regarding distribution semantics, serialization and fault tolerance. See [MBrace.Thespian.Tests](https://github.com/mbraceproject/MBrace.Core/tree/master/tests/MBrace.Thespian.Tests) and [MBrace.Azure.Tests](https://github.com/mbraceproject/MBrace.Azure/tree/master/tests/MBrace.Azure.Tests) for samples that make use of the test suites.

### MBrace.Thespian [![NuGet Status](http://img.shields.io/nuget/vpre/MBrace.Thespian.svg?style=flat)](https://www.nuget.org/packages/MBrace.Thespian/)

[MBrace.Thespian](https://github.com/mbraceproject/MBrace.Core/tree/master/samples/MBrace.Thespian) is a simple MBrace cluster implementation on top of MBrace.Runtime and the [Thespian](http://nessos.github.io/Thespian) actor library. Not intended for production deployments, it is used for testing MBrace core development.

## License

This project is subject to the Apache Licence, Version 2.0. 
A copy of the license can be found in [License.md](License.md) at the root of this repo.

## Maintainers

  * [@eiriktsarpalis](https://twitter.com/eiriktsarpalis)
  * [@nickpalladinos](https://twitter.com/nickpalladinos)
  * [@krontogiannis](https://twitter.com/krontogiannis)

## Build Status

Head (branch master), Build & Unit tests
  * Windows/.NET [![Build status](https://ci.appveyor.com/api/projects/status/3yaglw86q7vnja7w/branch/master?svg=true)](https://ci.appveyor.com/project/nessos/mbrace-core/branch/master)
  * Mac OS X/Mono 4.0 [![Build Status](https://travis-ci.org/mbraceproject/MBrace.Core.png?branch=master)](https://travis-ci.org/mbraceproject/MBrace.Core/branches)
