## MBrace.Core

[![Join the chat at https://gitter.im/mbraceproject/MBrace.Core](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/mbraceproject/MBrace.Core?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
[![NuGet Status](http://img.shields.io/nuget/vpre/MBrace.Core.svg?style=flat)](https://www.nuget.org/packages/MBrace.Core/)

This repository contains the foundations for the MBrace programming model and runtimes.

Prerelease packages are available on Nuget [[1](http://www.nuget.org/packages/MBrace.Core),[2](http://www.nuget.org/packages/MBrace.Runtime)].

### Contents

* [MBrace.Core](https://github.com/mbraceproject/MBrace.Core/tree/master/src/MBrace.Core): programming model essentials, cloud workflows, store primitives, libraries and client tools for authoring distributed code.
* [MBrace.CSharp](https://github.com/mbraceproject/MBrace.Core/tree/master/src/MBrace.Core): programming model essentials, cloud workflows, store primitives, libraries and client tools for authoring distributed code.
* [MBrace.Flow](https://github.com/mbraceproject/MBrace.Core/tree/master/src/MBrace.Flow): distributed [Streams](http://nessos.github.io/Streams/) implementation using MBrace.
* [MBrace.Core.Tests](https://github.com/mbraceproject/MBrace.Core/tree/master/tests/MBrace.Core.Tests): general-purpose test suites available to runtime implementers.
* [MBrace.Runtime](https://github.com/mbraceproject/MBrace.Core/tree/master/src/MBrace.Runtime): foundations and common implementations for developing MBrace runtimes.
* [MBrace.Thespian](https://github.com/mbraceproject/MBrace.Core/tree/master/samples/MBrace.Thespian): Sample MBrace runtime implementation using [Thespian](http://nessos.github.io/Thespian).

### Road map

See [ROADMAP.md](ROADMAP.md)

### Build Status

Head (branch master), Build & Unit tests
  * Windows/.NET [![Build status](https://ci.appveyor.com/api/projects/status/3yaglw86q7vnja7w/branch/master?svg=true)](https://ci.appveyor.com/project/nessos/mbrace-core/branch/master)
  * Mac OS X/Mono 4.0 [![Build Status](https://travis-ci.org/mbraceproject/MBrace.Core.png?branch=master)](https://travis-ci.org/mbraceproject/MBrace.Core/branches)
