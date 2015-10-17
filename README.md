[![Join the chat at https://gitter.im/mbraceproject/MBrace.Core](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/mbraceproject/MBrace.Core?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![NuGet Status](http://img.shields.io/nuget/vpre/MBrace.Core.svg?style=flat)](https://www.nuget.org/packages/MBrace.Core/)

# MBrace Core Libraries

This repository contains core libraries and runtime foundation components 
for the MBrace cloud programming model and frameworks.

For a first introduction to MBrace please refer to the main website at [m-brace.net](http://www.m-brace.net/).
If you have any questions regarding MBrace don't hesitate to create an issue or ask one of the [maintainers](#maintainers). 
You can also follow the official MBrace twitter account [@mbracethecloud](https://twitter.com/mbracethecloud).

## Repo Contents

### MBrace.Core [![NuGet Status](http://img.shields.io/nuget/vpre/MBrace.Core.svg?style=flat)](https://www.nuget.org/packages/MBrace.Core/)

[MBrace.Core](https://github.com/mbraceproject/MBrace.Core/tree/master/src/MBrace.Core) is a standalone class library that contains the core MBrace programming model, used to author general-purpose, runtime-agnostic distributed computation. It is centered on the concept of *cloud workflows*, a composable, language-integrated API based on F# computation expressions. It can be used to author specialized cloud libraries like MBrace.Flow.

### MBrace.Flow [![NuGet Status](http://img.shields.io/nuget/vpre/MBrace.Flow.svg?style=flat)](https://www.nuget.org/packages/MBrace.Flow/)

[MBrace.Flow](https://github.com/mbraceproject/MBrace.Core/tree/master/src/MBrace.Flow) is a distributed streaming library written on top of MBrace.Core. It enables distributed computation using functional pipeline declarations like the following:
```fsharp
CloudFlow.OfHttpFileByLine "http://my.server.local/large.txt"
|> CloudFlow.collect (fun line -> line.Split [|' '; ',' ; '.'|])
|> CloudFlow.filter (fun w -> w.Length > 3)
|> CloudFlow.map (fun w -> w.ToLower())
|> CloudFlow.countBy id
|> CloudFlow.sortBy (fun (_,c) -> -c) 10
|> CloudFlow.toArray
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

## MBrace Implementations

### MBrace.Azure [![NuGet Status](http://img.shields.io/nuget/vpre/MBrace.Azure.svg?style=flat)](https://www.nuget.org/packages/MBrace.Azure/)

[MBrace.Azure](http://github.com/mbraceproject/MBrace.Azure/) is an MBrace framework implementation on top of Azure PaaS components. Enables easy deployment of scalable MBrace clusters using worker roles. It also supports on-site cluster deployments using Azure storage/service bus components for communication.

## Building and Running unit tests

Building MBrace Core requires Visual Studio 2015/F# 4.0. You can build the project either from Visual Studio or by running `build.cmd Build` if using cmd/powershell or `./build.sh Build` if using bash/sh.

Unit tests can be run by calling `build.cmd RunTests` or `./build.sh RunTests`. 
You can skip the time consuming Thespian by adding the `-ef IgnoreClusterTests` flag in the above commands.
Alternatively you can run individual tests by opening test assemblies found in the repository's `/bin` folder
using [NUnit-GUI](http://www.nunit.org/index.php?p=nunit-gui&r=2.2.10). Building the solution produces the following
test assemblies:
  * `MBrace.Core.Tests.dll` for testing the core MBrace library.
  * `MBrace.Runtime.Tests.dll` for testing MBrace.Runtime components.
  * `MBrace.Thespian.Tests.dll` for testing the MBrace Thespian implementation.

When performing changes to MBrace.Core, MBrace.Flow or MBrace.Runtime it is essential to verify that the MBrace.Thespian tests are still passing. 

## Contributing

The MBrace project is happy to accept quality contributions from the .NET community.
If you would like to get involved, here are a few places you could have a look at:
  * MBrace Libraries: we are looking for data scientists and domain experts who can help us develop specialized libraries that run on top of MBrace.Core. Examples include [Machine Learning](https://spark.apache.org/docs/1.1.0/mllib-guide.html) and [Graph analytics](http://spark.apache.org/graphx/) libraries.
  * MBrace Runtimes: help MBrace grow by extending support to your favorite cloud service. It could be AWS, YARN/HDFS or your private infrastructure.
  * C# Support: As of MBrace 1.0, development of [MBrace.CSharp](#mbrace.csharp) has been suspended. We are looking for working C# developers interested in extending MBrace support to C# and testing deployments using the upcoming C# Interactive.
  * Documentation & Code Samples: help improve MBrace documentation and coding samples. See the [MBrace.StarterKit](https://github.com/mbraceproject/MBrace.StarterKit) for current coding samples and the [mbrace-docs](https://github.com/mbraceproject/mbrace-docs) repo for documentation and the [m-brace.net](http://www.m-brace.net/) website.

## License

This project is subject to the [Apache Licence, Version 2.0](License.md).

## Maintainers

  * [@eiriktsarpalis](https://twitter.com/eiriktsarpalis)
  * [@nickpalladinos](https://twitter.com/nickpalladinos)
  * [@krontogiannis](https://twitter.com/krontogiannis)

## Build Status

Head (branch master), Build & Unit tests
  * Windows/.NET [![Build status](https://ci.appveyor.com/api/projects/status/3yaglw86q7vnja7w/branch/master?svg=true)](https://ci.appveyor.com/project/nessos/mbrace-core/branch/master)
  * Mac OS X/Mono 4.0 [![Build Status](https://travis-ci.org/mbraceproject/MBrace.Core.png?branch=master)](https://travis-ci.org/mbraceproject/MBrace.Core/branches)
