namespace MBrace.Thespian.Runtime

open MBrace.Library

open MBrace.Runtime
open MBrace.Runtime.Store

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
    }

/// Distributable, one-time local state factory
and LocalStateFactory = DomainLocal<LocalState>