module Nessos.MBrace.Runtime.Serialization

    /// Represents a typed serialized value.
    type Pickle<'T> private (data : byte []) =
        new (value : 'T) = new Pickle<'T>(VagrantRegistry.Pickler.Pickle(value))
        member __.UnPickle () = VagrantRegistry.Pickler.UnPickle<'T> data
        member __.Data = data

    /// Pickle methods
    [<RequireQualifiedAccess>]
    module Pickle =

        /// Pickles a value.
        let pickle value = new Pickle<'T>(value)
        /// Unpickles a value.
        let unpickle (p : Pickle<'T>) = p.UnPickle()