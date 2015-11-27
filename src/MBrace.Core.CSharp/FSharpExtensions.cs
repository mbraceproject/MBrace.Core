using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.FSharp.Core;
using MBrace.Core.Internals.CSharpProxy;

namespace MBrace.CSharp
{
    using unit = Microsoft.FSharp.Core.Unit;

    /// <summary>
    ///     Collection of C# wrappers for F# types
    /// </summary>
    public static class FSharpExtensions
    {
        /// <summary>
        ///     Wraps value to an FSharpOption type
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="value">Input value.</param>
        /// <returns>FSharpOption.Some wrapper</returns>
        public static FSharpOption<T> ToOption<T>(this T value)
        {
            return FSharpOption<T>.Some(value);
        }

        /// <summary>
        ///     Wraps delegate to an F# function
        /// </summary>
        /// <typeparam name="T">Return type.</typeparam>
        /// <param name="func">Input delegate.</param>
        /// <returns>An F# lambda wrapper.</returns>
        public static FSharpFunc<unit, T> ToFSharpFunc<T>(this Func<T> func)
        {
            return FSharpFunc.Create(func);
        }

        /// <summary>
        ///     Wraps delegate to an F# function
        /// </summary>
        /// <typeparam name="S">Argument type.</typeparam>
        /// <typeparam name="T">Return type.</typeparam>
        /// <param name="func">Input delegate.</param>
        /// <returns>An F# lambda wrapper.</returns>
        public static FSharpFunc<S,T> ToFSharpFunc<S,T>(this Func<S,T> func)
        {
            return FSharpFunc.Create(func);
        }

        /// <summary>
        ///     Wraps delegate to an F# function
        /// </summary>
        /// <typeparam name="S1"></typeparam>
        /// <typeparam name="S2"></typeparam>
        /// <typeparam name="T">Return type.</typeparam>
        /// <param name="func">Input delegate.</param>
        /// <returns>An F# lambda wrapper.</returns>
        public static FSharpFunc<S1,FSharpFunc<S2,T>> ToFSharpFunc<S1,S2,T>(this Func<S1,S2,T> func)
        {
            return FSharpFunc.Create(func);
        }

        /// <summary>
        ///     Wraps delegate to an F# function
        /// </summary>
        /// <typeparam name="S1"></typeparam>
        /// <typeparam name="S2"></typeparam>
        /// <typeparam name="S3"></typeparam>
        /// <typeparam name="T">Return type.</typeparam>
        /// <param name="func">Input delegate.</param>
        /// <returns>An F# lambda wrapper.</returns>
        public static FSharpFunc<S1,FSharpFunc<S2,FSharpFunc<S3,T>>> ToFSharpFunc<S1,S2,S3,T>(this Func<S1,S2,S3,T> func)
        {
            return FSharpFunc.Create(func);
        }

        /// <summary>
        ///     Wraps delegate to an F# function
        /// </summary>
        /// <typeparam name="S1"></typeparam>
        /// <typeparam name="S2"></typeparam>
        /// <typeparam name="S3"></typeparam>
        /// <typeparam name="S4"></typeparam>
        /// <typeparam name="T">Return type.</typeparam>
        /// <param name="func">Input delegate.</param>
        /// <returns>An F# lambda wrapper.</returns>
        public static FSharpFunc<S1, FSharpFunc<S2, FSharpFunc<S3, FSharpFunc<S4, T>>>> ToFSharpFunc<S1, S2, S3, S4, T>(this Func<S1, S2, S3, S4, T> func)
        {
            return FSharpFunc.Create(func);
        }

        /// <summary>
        ///     Wraps delegate to an F# function
        /// </summary>
        /// <param name="func">Input delegate.</param>
        /// <returns>An F# lambda wrapper.</returns>
        public static FSharpFunc<unit, unit> ToFSharpFunc(this Action func)
        {
            return FSharpFunc.Create(func);
        }

        /// <summary>
        ///     Wraps delegate to an F# function
        /// </summary>
        /// <typeparam name="S"></typeparam>
        /// <param name="func">Input delegate.</param>
        /// <returns>An F# lambda wrapper.</returns>
        public static FSharpFunc<S, unit> ToFSharpFunc<S>(this Action<S> func)
        {
            return FSharpFunc.Create(func);
        }

        /// <summary>
        ///     Wraps delegate to an F# function
        /// </summary>
        /// <typeparam name="S1"></typeparam>
        /// <typeparam name="S2"></typeparam>
        /// <param name="func">Input delegate.</param>
        /// <returns>An F# lambda wrapper.</returns>
        public static FSharpFunc<S1, FSharpFunc<S2, unit>> ToFSharpFunc<S1,S2>(this Action<S1,S2> func)
        {
            return FSharpFunc.Create(func);
        }

        /// <summary>
        ///     Wraps delegate to an F# function
        /// </summary>
        /// <typeparam name="S1"></typeparam>
        /// <typeparam name="S2"></typeparam>
        /// <typeparam name="S3"></typeparam>
        /// <param name="func">Input delegate.</param>
        /// <returns>An F# lambda wrapper.</returns>
        public static FSharpFunc<S1, FSharpFunc<S2, FSharpFunc<S3, unit>>> ToFSharpFunc<S1, S2, S3>(this Action<S1, S2, S3> func)
        {
            return FSharpFunc.Create(func);
        }
    }
}
