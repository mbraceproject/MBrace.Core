namespace Nessos.MBrace.InMemory

    open Nessos.MBrace
    open Nessos.MBrace.Runtime

    type InMemory private () =
    
        static let imemResource =
            resource { 
                yield InMemoryScheduler.Create() :> ISchedulingProvider
                yield InMemoryStorageProvider.Create() :> IStorageProvider
            }

        static member Resource = imemResource