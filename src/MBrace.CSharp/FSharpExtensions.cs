using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.FSharp.Core;
using MBrace.Core.Internals.CSharpProxy;

namespace MBrace.Core.CSharp
{
    using unit = Microsoft.FSharp.Core.Unit;

    /// <summary>
    ///     Optional type helper methods
    /// </summary>
    public static class Option
    {
        /// <summary>
        ///     Checks whether provided optional parameter is 'None'
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="optional">Input optional parameter.</param>
        /// <returns>False if optional has value, True otherwise.</returns>
        public static bool IsNone<T>(FSharpOption<T> optional)
        {
            return FSharpOption<T>.get_IsNone(optional);
        }

        /// <summary>
        ///     Checks whether provided optional parameter is 'Some'
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="optional">Input optional parameter.</param>
        /// <returns>True if optional has value, False otherwise.</returns>
        public static bool IsSome<T>(FSharpOption<T> optional)
        {
            return FSharpOption<T>.get_IsSome(optional);
        }

        /// <summary>
        ///     Constructs an optional with provided input value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="value">Input value.</param>
        /// <returns>Returns an optional that contains input value.</returns>
        public static FSharpOption<T> Some<T>(T value)
        {
            return FSharpOption<T>.Some(value);
        }

        /// <summary>
        ///     Constructs an optional without value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public static FSharpOption<T> None<T>()
        {
            return FSharpOption<T>.None;
        }

        /// <summary>
        ///     Gets the value for supplied optional.
        ///     Will raise exception if None.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="optional">Input optional container.</param>
        /// <returns>Contained value in optional, if it exists.</returns>
        public static T Get<T>(FSharpOption<T> optional)
        {
            return OptionModule.GetValue<T>(optional);
        }
    }

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
            return Option.Some(value);
        }

        /// <summary>
        ///     Try getting result from F# optional value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="optional">Optional container.</param>
        /// <param name="result">Input result.</param>
        /// <returns>True if Some value, False if None.</returns>
        public static bool TryGetValue<T> (this FSharpOption<T> optional, out T result)
        {
            if (Option.IsSome(optional))
            {
                result = optional.Value;
                return true;
            }

            result = default(T);
            return false;
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
