#### 1.0.5
* CloudFlow bugfix.

#### 1.0.4
* Update Vagabond dependencies.

#### 1.0.3
* Minor FileSystem fixes.

#### 1.0.2
* Improve C# interactive support.

#### 1.0.1
* CloudFlow performance improvements.
* Improve C# interactive support.
* Add CloudFlow.averageByKey.

#### 1.0.0
* MBrace.Core 1.0 release.
* C# interop improvements.

#### 0.16.0-beta
* Add support for text-based serializers in MBrace.Core
* Expose text-based serializer APIs in MBrace.Core
* Add CloudProcess.Wait, .WaitAny and .WaitAll methods. 

#### 0.15.3-beta
* PersistedSequence performance improvements.

#### 0.15.2-beta
* WorkerRef pretty printing performance improvements.

#### 0.15.1-beta
* ICloudCollection performance fixes and improvements.

#### 0.15.0-beta
* CloudFlow performance fixes and improvements.

#### 0.14.1-beta
* Misc bugfixes and improvements.

#### 0.14.0-beta
* Add CloudFile.UploadFromStream and CloudFile.DownloadToStream methods.
* Add ICloudFileStore.Upload and .Download methods.
* Fix CloudFlow sortBy issue.
* Rename CloudLocal<T> to LocalCloud<T>.

#### 0.13.2-beta
* Misc improvements and bugfixes

#### 0.13.1-beta
* Clean up MBrace.Core Async extensions

#### 0.13.0-beta
* Rename Local<'T> workflows to CloudLocal<'T>.
* Refactor store primitive APIs to only use asynchronous and synchronous methods.
* Make async bindings to cloud builders optional.
* Remove all CloudFlow.*Local combinators.
* Implement CloudFlow.peek.
* Refactor StoreClient objects and make them serializable.
* MBrace.Core bugfixes and improvements.

#### 0.12.2-beta
* Add support for forced local FSharp.Core binding redirects.

#### 0.12.1-beta
* Fix packaging issue.

#### 0.12.0-beta
* Revise and consolidate API names.
* Bugfix in CloudProcess client reporting code.
* Package MBrace.Thespian with FSharp.Core 4.4.0.0.

#### 0.11.12-alpha
* Minor bugfix.

#### 0.11.11-alpha
* Fix CloudQueue unit test issue.

#### 0.11.10-alpha
* Improve FaultPolicy implementation, add FaultPolicy getter,setter in MBraceClient instances.

#### 0.11.9-alpha
* Refactor CloudAtom, CloudDictionary and CloudQueue core APIs to support named lookups.
* Minor improvements in MBrace.Runtime types.

#### 0.11.8-alpha
* Add support for working directory prefixes.
* File logger bugfix.

#### 0.11.7-alpha
* Minor bugfix.

#### 0.11.6-alpha
* Expose worker performance submission intervals.

#### 0.11.5-alpha
* Add support for worker specifiable heartbeat intervals.

#### 0.11.4-alpha
* Use longs in ICloudCounter.

#### 0.11.3-alpha
* Minor fixes and improvements.

#### 0.11.2-alpha
* Implement compression support for CloudFiles and PersistedCloudValues.
* Implement compression support for StoreAssemblyManager.
* Implement compression support for StoreCloudValueProvider.
* Use DateTimeOffset in WorkerRef and CloudProcess properties.

#### 0.11.1-alpha
* Fix packaging issue in MBrace.Thespian.

#### 0.11.0-alpha
* Fix CloudProcess serialization issue.
* Fix StructuredFormatDisplay issues in MBrace primitives.

#### 0.10.10-alpha
* Rename CloudTask<_> to CloudProcess<_>.
* Fix Vagabond issue in Mixed Mode assemblies.
* Implement MBrace.Library.Protected combinators.

#### 0.10.9-alpha
* Refactor FaultPolicy using interfaces.
* Implement Cloud.IsPreviouslyFaulted property.

#### 0.10.8-alpha
* Fix task counter issue in faulted work items.

#### 0.10.7-alpha
* Use DateTimeOffset in place of DateTime in log entries.
* Extend and improve logging abstractions for MBrace clients.

#### 0.10.6-alpha
* StoreAssemblyManager tweaks.
* Implement fetching of worker logs on the client side.
* Minor API revisions and fixes.

#### 0.10.5-alpha
* Improve VagabondRegistry support for multiple clients in single process.
* Rename CloudFile instances to CloudFileInfo.
* Rename CloudDirectory instances to CloudDirectoryInfo.

#### 0.10.4-alpha
* Core improvements and cleanups.
* Add icon to Thespian worker executable.

#### 0.10.3-alpha
* Sifting bugfixes.

#### 0.10.2-alpha
* MBrace.Runtime WorkerRef and CloudTask fixes.
* Make MBrace.Thespian class library and keep worker exe in separate project.

#### 0.10.1-alpha
* Add methods for native dependency registration in client.
* Add support for specifying runtime resources per cloudtask.
* Add new logging tools.

#### 0.10.0-alpha
* Implement CloudValue as interface that supports StorageLevels.
* Refactor PersistedCloudFlow to use CloudValue as underlying storage.
* Implement Closure serialization optimizations.
* Implement CloudFlow.OfHttpFileByLine producers.
* Improve MBrace.Thespian implementation.
* Multiple bugfixes and overall improvements.

#### 0.9.14-alpha
* Fix packaging issue.

#### 0.9.13-alpha
* Allow binding to async workflows in cloud workflows.
* Expand MBrace.Runtime.Core project.

#### 0.9.12-alpha
* Bugfixes in partitioner.

#### 0.9.11-alpha
* Add more combinators in MBrace.Workflows.
* Improvide FileStore client API and helper methods
* Add DomainLocal utility.
* Misc bugfixes.

#### 0.9.10-alpha
* Improve partitioning in CloudFlow.
* Add missing combinators to CloudFlow.
* Add ETags to FileStore API.
* Rename CloudVector to PersistedCloudFlow.

#### 0.9.9-alpha
* Introduce ICloudCollection and ICloudDictionary.
* Refactor namespaces in MBrace.Core.
* Refactor CloudVector and MBrace.Flow.
* Rename CloudCell to CloudValue.
* Misc revisions and improvements.

#### 0.9.8-alpha
* Upgrade to Vagabond 0.6.
* Rename MBrace.Streams to MBrace.Flow.

#### 0.9.7-alpha
* Add 'cacheByDefault' setting to cacheable entitites.
* Implement CloudStream.*Local combinators.
* Upgrade Vagabond.

#### 0.9.6-alpha
* Add text methods to MBrace.Streams.
* Refine CloudTask cancellation.
* Update Vagabond to 0.5.0.
* Minor bugfixes.

#### 0.9.5-alpha
* Refine local workflows implementation.
* Improve MBrace.Streams implementations.
* Add CloudCacheable<T> primitive.
* Improve DivideAndConquer workflows.
* Misc fixes and improvements.

#### 0.9.4-alpha
* Packaging hotfix.

#### 0.9.3-alpha
* Introduce Local<> workflows to programming model.
* Add IWorkerRef.ProcessorCount property.

#### 0.9.2-alpha
* Release MBrace.Streams NuGet package.
* Minor revisions and bugfixes.

#### 0.9.1-alpha
* Add task/cancellation token support in the core library.
* Minor revisions and bugfixes.

#### 0.9.0-alpha
* MBrace.Core initial prerelease.