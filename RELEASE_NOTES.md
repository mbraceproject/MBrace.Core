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