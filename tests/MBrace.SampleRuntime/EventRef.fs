namespace Nessos.MBrace.SampleRuntime

open System
open System.Collections.Concurrent

/// Represents a global, serializable event reference that 
/// can be instantiated to process-bound event handling.
[<AutoSerializable(true)>]
type internal EventRef<'T> () =
    // event ref global identifier
    let id = System.Guid.NewGuid().ToString()
    // local event registry by id.
    static let localEvents = new ConcurrentDictionary<string, Event<'T>>()

    /// <summary>
    ///     Installs a local event instance in current process.
    /// </summary>
    /// <returns>Uninstallation IDisposable</returns>
    member __.InstallLocalEvent () : IDisposable =
        let _ = localEvents.GetOrAdd(id, fun _ -> new Event<'T> ())
        {
            new IDisposable with
                member __.Dispose () = localEvents.TryRemove id |> ignore
        }

    /// <summary>
    ///     Triggers a local event, if installed in the current process.
    /// </summary>
    /// <param name="value">Trigger value.</param>
    member __.Trigger value =
        let ok, e = localEvents.TryGetValue id
        if ok then e.Trigger value
        else
            invalidOp "No local event has been registered."

    /// <summary>
    ///     Publishes an observable to the local event, if installed in the current process.
    /// </summary>
    member __.Publish =
        let ok, e = localEvents.TryGetValue id
        if ok then e.Publish
        else
            invalidOp "No local event has been registered."