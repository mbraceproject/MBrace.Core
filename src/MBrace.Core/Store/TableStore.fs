namespace Nessos.MBrace.Store

/// Defines a cloud table storage abstraction
type ICloudTableStore =

    /// Unique table store identifier
    abstract UUID : string

    /// <summary>
    ///     Checks if provided value is suitable for table storage
    /// </summary>
    /// <param name="value">Value to be checked.</param>
    abstract IsSupportedValue : value:'T -> bool

    /// <summary>
    ///     Checks if entry with provided key exists.
    /// </summary>
    /// <param name="id">Entry id.</param>
    abstract Exists : id:string -> Async<bool>

    /// <summary>
    ///     Deletes entry with provided key.
    /// </summary>
    /// <param name="id"></param>
    abstract Delete : id:string -> Async<unit>

    /// <summary>
    ///     Creates a new entry with provided initial value.
    ///     Returns the key identifier for new entry.
    /// </summary>
    /// <param name="initial">Initial value.</param>
    abstract Create<'T> : initial:'T -> Async<string>

    /// <summary>
    ///     Returns the current value of provided atom.
    /// </summary>
    /// <param name="id">Entry identifier.</param>
    abstract GetValue<'T> : id:string -> Async<'T>

    /// <summary>
    ///     Atomically updates table entry of given id using updating function.
    /// </summary>
    /// <param name="id">Entry identifier.</param>
    /// <param name="updater">Updating function.</param>
    abstract Update : id:string * updater:('T -> 'T) -> Async<unit>

    /// <summary>
    ///     Force update of existing table entry with given value.
    /// </summary>
    /// <param name="id">Entry identifier.</param>
    /// <param name="value">Value to be set.</param>
    abstract Force : id:string * value:'T -> Async<unit>

    /// <summary>
    ///     Enumerates all keys.
    /// </summary>
    abstract EnumerateKeys : unit -> Async<string>