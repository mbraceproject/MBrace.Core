using Microsoft.FSharp.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Nessos.MBrace.Core.CSharp
{

    // F# <-> C# helpers

    internal static class Utils
    {
        internal static FSharpFunc<T,Nessos.MBrace.Cloud<U>> AsFSharpFunc<T,U> (this Func<T,Cloud<U>> f)
        {
            return FSharpFunc<T, Nessos.MBrace.Cloud<U>>.FromConverter(t => f(t).Computation);
        }

        internal static FSharpFunc<Unit, Nessos.MBrace.Cloud<T>> AsFSharpFunc<T>(this Func<Cloud<T>> f)
        {
            return FSharpFunc<Unit, Nessos.MBrace.Cloud<T>>.FromConverter(_ => f().Computation);
        }

    }
}
