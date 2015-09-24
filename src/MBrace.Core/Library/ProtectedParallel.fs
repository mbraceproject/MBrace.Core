namespace MBrace.Library.Protected

open MBrace.Core
open MBrace.Core.Internals

/// Protected computation result coming from a child computation in a Cloud.Parallel workflow.
[<NoEquality; NoComparison>]
type ProtectedResult<'T> =
    /// Computation successfully completed with value.
    | Value of 'T
    /// Computation raised user-level exception.
    | UserException of exn
    /// Computation has faulted at the system level.
    | FaultException of exn

type Cloud =

    /// <summary>
    ///     Variation of the Cloud.Parallel workflow which protects the workflow from child computation
    ///     user exceptions and faults. The computation will always complete succesfully even with partial results.
    /// </summary>
    /// <param name="computations">Input child computations.</param>
    static member ProtectedParallel(computations : seq<Cloud<'T>>) : Cloud<ProtectedResult<'T> []> = cloud {
        let protect (w : Cloud<'T>) = cloud {
            let! faultState = Cloud.TryGetFaultData()
            match faultState with
            | Some fault -> return FaultException fault.FaultException
            | None ->
                try let! r = w in return Value r
                with e -> return UserException e
        }

        return! 
            computations 
            |> Seq.map protect 
            |> Cloud.Parallel
            |> Cloud.WithFaultPolicy (FaultPolicy.WithMaxRetries 1)
    }

    /// <summary>
    ///     Variation of the Cloud.Choice workflow which protects the workflow from child computation
    ///     use exceptions and faults. The computation will always complete successfully once one of the
    ///     children has completed successfully.
    /// </summary>
    /// <param name="computations">Input child computations.</param>
    static member ProtectedChoice(computations : seq<Cloud<'T option>>) : Cloud<'T option> = cloud {
        let protect (w : Cloud<'T option>) = cloud {
            let! faultState = Cloud.TryGetFaultData()
            match faultState with
            | Some _ -> return None
            | None ->
                try return! w
                with _ -> return None
        }
    
        return! 
            computations 
            |> Seq.map protect 
            |> Cloud.Choice
            |> Cloud.WithFaultPolicy (FaultPolicy.WithMaxRetries 1)
    }