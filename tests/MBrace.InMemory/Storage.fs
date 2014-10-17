namespace Nessos.MBrace.InMemory

    open Nessos.MBrace
    open Nessos.MBrace.Runtime

    type internal InMemoryStorageProvider private () =

        static member Create() = new InMemoryStorageProvider()

        interface IStorageProvider with
            member __.CreateCloudRef(value : 'T) = async {
                let id = System.Guid.NewGuid().ToString()
                return {
                    new obj() with
                        override __.ToString() = sprintf "cloudref:%s" id

                    interface ICloudRef<'T> with
                        member __.Id = id
                        member __.GetValue () = async { return value }
                        member __.Dispose () = async.Zero()
                }
            }