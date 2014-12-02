namespace Nessos.MBrace.Azure.Store

open System
open System.IO
open System.Security.AccessControl
open System.Runtime.Serialization

open Nessos.MBrace.Store
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Table

/// Store implementation that uses a filesystem as backend.
[<Sealed;AutoSerializable(false)>]
type TableStore (conn : string) =
    
    let acc = CloudStorageAccount.Parse(conn)
    let defaultTable = "mbrace"

    interface ICloudTableStore with
        
        member x.UUID: string = acc.TableStorageUri.ToString()

        member x.Create(initial: 'T): Async<string> = 
            async {
                let binary = serialize initial 
                let id = Guid.NewGuid().ToString()
                let e = new FatEntity(id, binary)
                let! _ = Table.insert<FatEntity> acc defaultTable e
                return id
            }
        
        member x.Delete(id: string): Async<unit> = 
            async {
                let! e = Table.read<FatEntity> acc defaultTable id String.Empty
                return! Table.delete<FatEntity> acc defaultTable e
            }
        
        member x.EnumerateKeys(): Async<string []> = 
            async {
                let table = Clients.getTableClient(acc).GetTableReference(defaultTable)
                let! exists = Async.AwaitTask(table.ExistsAsync())
                if exists then
                    let rangeQuery = TableQuery<DynamicTableEntity>().Select([|"PartitionKey"|])
                    let resolver = EntityResolver(fun pk rk ts props etag -> pk)
                    return table.ExecuteQuery(rangeQuery, resolver, null, null)
                           |> Seq.toArray
                else 
                    return Array.empty
            }
        
        member x.Exists(id: string): Async<bool> = 
            async {
                let! e = Table.read<FatEntity> acc defaultTable id String.Empty
                return e <> null
            }
        
        member x.Update(id: string, updater: 'T -> 'T): Async<unit> = 
            async {
                let rec update () = async {
                    let! e = Table.read<FatEntity> acc defaultTable id String.Empty
                    let oldValue = deserialize(e.GetPayload())
                    let newValue = updater oldValue
                    let newBinary = serialize newValue
                    let e = new FatEntity(e.PartitionKey, newBinary, ETag = e.ETag)
                    let! result = Async.Catch <| Table.merge acc defaultTable e
                    match result with
                    | Choice1Of2 _ -> return ()
                    | Choice2Of2 e when Table.PreconditionFailed e -> return! update()
                    | Choice2Of2 e -> return raise e
                }
                return! update ()
            }       

        member x.Force(id: string, newValue: 'T): Async<unit> = 
            async {
                let! e = Table.read<FatEntity> acc defaultTable id String.Empty
                let newBinary = serialize newValue
                let e = new FatEntity(e.PartitionKey, newBinary, ETag = "*")
                let! _ = Table.merge acc defaultTable e
                return ()
            }       


        member x.GetValue(id: string): Async<'T> = 
            async {
                let! e = Table.read<FatEntity> acc defaultTable id String.Empty
                let value = deserialize(e.GetPayload())
                return value
            }
        
        member x.IsSupportedValue(value: 'T): bool = isFatEntity value
        
        member x.GetFactory(): ICloudTableStoreFactory = 
            let c = conn
            { new ICloudTableStoreFactory with
                  member x.Create() : ICloudTableStore = new TableStore(c) :> _ }