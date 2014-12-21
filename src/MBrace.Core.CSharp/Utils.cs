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
        internal static FSharpFunc<T,U> AsFSharpFunc<T,U> (this Func<T,U> f)
        {
            return FSharpFunc<T, U>.FromConverter(t => f(t));
        }

        internal static FSharpFunc<Unit, T> AsFSharpFunc<T>(this Func<T> f)
        {
            return FSharpFunc<Unit, T>.FromConverter(_ => f());
        }

    }
}
