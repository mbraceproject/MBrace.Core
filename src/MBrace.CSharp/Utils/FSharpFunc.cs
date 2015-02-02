using Microsoft.FSharp.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MBrace.CSharp
{
    internal static class FSharpFuncExtensions
    {
        internal static FSharpFunc<T,MBrace.Cloud<U>> AsFSharpFunc<T,U> (this Func<T,Cloud<U>> f)
        {
            return FSharpFunc<T, MBrace.Cloud<U>>.FromConverter(t => f(t));
        }

        internal static FSharpFunc<Unit, MBrace.Cloud<T>> AsFSharpFunc<T>(this Func<Cloud<T>> f)
        {
            return FSharpFunc<Unit, MBrace.Cloud<T>>.FromConverter(_ => f());
        }

        internal static FSharpFunc<T, U> AsFSharpFunc<T, U>(this Func<T,U> f)
        {
            return FSharpFunc<T,U>.FromConverter(t => f(t));
        }

    }
}
