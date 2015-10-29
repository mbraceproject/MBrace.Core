namespace MBrace.Thespian.Runtime

open MBrace.Library

open MBrace.Runtime
open MBrace.Runtime.Components

/// Result produced by mbrace cluster to be serialized
type ResultMessage<'T> = PickleOrFile<SiftedClosure<'T>>

/// Contains state specific to current node in MBrace cluster
[<AutoSerializable(false); NoEquality; NoComparison>]
type LocalState =
    {
        /// Root logger instace used for subscribing user-supplied loggers
        Logger : AttacheableLogger
        /// Local SiftManager instance
        SiftManager : ClosureSiftManager
        /// Local AssemblyManager instance
        AssemblyManager : StoreAssemblyManager
        /// Local AssemblyManager instance
        PersistedValueManager : PersistedValueManager
        /// Local system log manager instance
        SystemLogManager : StoreSystemLogManager
        /// Local cloud log manager instance
        CloudLogManager : StoreCloudLogManager
    }

    member ls.CreateResult(t : 'T, allowNewSifts : bool, fileName : string) : Async<ResultMessage<'T>> = async {
        let! sift = ls.SiftManager.SiftClosure(t, allowNewSifts)
        return! ls.PersistedValueManager.CreatePickleOrFileAsync(sift, fileName)
    }

    member ls.ReadResult(result : ResultMessage<'T>) = async {
        let! sift = ls.PersistedValueManager.ReadPickleOrFileAsync result
        return! ls.SiftManager.UnSiftClosure sift
    }

/// Distributable, one-time local state factory
and LocalStateFactory = DomainLocal<LocalState>