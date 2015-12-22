using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.FSharp.Core;
using MBrace.Core.Internals.CSharpProxy;

using unit = Microsoft.FSharp.Core.Unit;

namespace MBrace.Core.CSharp
{
    /// <summary>
    ///     Cloud Workflow extension methods
    /// </summary>
    public static class CloudBuilder
    {
        #region Simple Cloud Factories

        /// <summary>
        ///     Defines a cloud workflow that performs no operation
        /// </summary>
        public static LocalCloud<unit> Empty
        {
            get { return Builders.local.Zero(); }
        }

        /// <summary>
        ///     Creates a cloud workflow that simply returns the supplied value.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="value">Input value.</param>
        /// <returns>Cloud workflow that returns given value.</returns>
        public static LocalCloud<T> FromValue<T>(T value)
        {
            return Builders.local.Return(value);
        }

        /// <summary>
        ///     Creates a cloud workflow that simply throws the provided exception.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="exception">Exception to be thrown.</param>
        /// <returns></returns>
        public static LocalCloud<T> Throw<T>(Exception exception)
        {
            return Core.Cloud.Raise<T>(exception);
        }

        /// <summary>
        ///     Creates a cloud workflow that simply encapsulates the supplied delegate.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="func">Input function.</param>
        /// <returns>Cloud workflow that executes the given value.</returns>
        public static LocalCloud<T> FromFunc<T>(Func<T> func)
        {
            var l = Builders.local;
            return l.Delay(FSharpFunc.Create(() => l.Return(func.Invoke())));
        }

        /// <summary>
        ///     Creates a cloud workflow that simply encapsulates the supplied delegate.
        /// </summary>
        /// <typeparam name="S"></typeparam>
        /// <typeparam name="T"></typeparam>
        /// <param name="func">Input function.</param>
        /// <returns>Cloud workflow that executes the given value.</returns>
        public static Func<S, LocalCloud<T>> FromFunc<S,T>(Func<S,T> func)
        {
            var l = Builders.local;
            return (x => l.Delay(FSharpFunc.Create(() => l.Return(func.Invoke(x)))));
        }

        /// <summary>
        ///     Creates a cloud workflow that simply encapsulates the supplied delegate.
        /// </summary>
        /// <param name="func">Input function.</param>
        /// <returns>Cloud workflow that executes the given value.</returns>
        public static LocalCloud<unit> FromFunc(Action func)
        {
            var l = Builders.local;
            return l.Delay(FSharpFunc.Create(() => { func.Invoke(); return l.Zero(); }));
        }

        /// <summary>
        ///     Creates a cloud workflow that simply encapsulates the supplied delegate.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="func">Input function.</param>
        /// <returns>Cloud workflow that executes the given value.</returns>
        public static Func<T, LocalCloud<unit>> FromFunc<T>(Action<T> func)
        {
            var l = Builders.local;
            return (x => l.Delay(FSharpFunc.Create(() => { func.Invoke(x); return l.Zero(); })));
        }

        /// <summary>
        ///     Wraps computation workflow builder function
        ///     to a cloud computation that delays builder execution in the cloud.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="func">Cloud workflow builder function to be delayed.</param>
        /// <returns></returns>
        public static LocalCloud<T> Delay<T>(Func<LocalCloud<T>> func)
        {
            return Builders.local.Delay(FSharpFunc.Create(func));
        }

        /// <summary>
        ///     Wraps computation workflow builder function
        ///     to a cloud computation that delays builder execution in the cloud.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="func">Cloud workflow builder function to be delayed.</param>
        /// <returns></returns>
        public static Cloud<T> Delay<T>(Func<Cloud<T>> func)
        {
            return Builders.cloud.Delay(FSharpFunc.Create(func));
        }

        /// <summary>
        ///     Defines a cloud workflow that writes given formatted string to cluster logs.
        /// </summary>
        /// <param name="format">String format.</param>
        /// <param name="paramList">Optional parameters.</param>
        /// <returns>Cloud workflow that performs a logging operation.</returns>
        public static LocalCloud<unit> Log(string format, params object[] paramList)
        {
            return Core.Cloud.Log(string.Format(format, paramList));
        }

        /// <summary>
        ///     Defines a cloud workflow that sleeps asynchronously for given amount of time.
        /// </summary>
        /// <param name="millisecondsDue">Total milliseconds to sleep.</param>
        /// <returns></returns>
        public static LocalCloud<unit> Sleep(int millisecondsDue)
        {
            return Core.Cloud.Sleep(millisecondsDue);
        }

        #endregion

        #region Extension Methods

        /// <summary>
        ///     Binds result of provided workflow to supplied continuation lambda.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="S"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="cont">Result continuation.</param>
        /// <returns>A cloud workflow composed of the two.</returns>
        public static Cloud<S> Bind<T, S>(this Cloud<T> workflow, Func<T, Cloud<S>> cont)
        {
            return Builders.cloud.Bind(workflow, FSharpFunc.Create(cont));
        }

        /// <summary>
        ///     Binds result of provided workflow to supplied continuation lambda.
        /// </summary>
        /// <typeparam name="S"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="cont">Result continuation.</param>
        /// <returns>A cloud workflow composed of the two.</returns>
        public static Cloud<S> Bind<S>(this Cloud<unit> workflow, Func<Cloud<S>> cont)
        {
            return Builders.cloud.Bind(workflow, FSharpFunc.Create(cont));
        }

        /// <summary>
        ///     Binds result of provided workflow to supplied continuation lambda.
        /// </summary>
        /// <typeparam name="T1"></typeparam>
        /// <typeparam name="T2"></typeparam>
        /// <typeparam name="S"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="cont">Result continuation.</param>
        /// <returns>A cloud workflow composed of the two.</returns>
        public static Cloud<S> Bind<T1,T2,S>(this Cloud<Tuple<T1, T2>> workflow, Func<T1, T2, Cloud<S>> cont)
        {
            return CloudBuilder.Bind(workflow, (tuple => cont.Invoke(tuple.Item1, tuple.Item2)));
        }

        /// <summary>
        ///     Binds result of provided workflow to supplied continuation lambda.
        /// </summary>
        /// <typeparam name="T1"></typeparam>
        /// <typeparam name="T2"></typeparam>
        /// <typeparam name="T3"></typeparam>
        /// <typeparam name="S"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="cont">Result continuation.</param>
        /// <returns>A cloud workflow composed of the two.</returns>
        public static Cloud<S> Bind<T1, T2, T3, S>(this Cloud<Tuple<T1, T2, T3>> workflow, Func<T1, T2, T3, Cloud<S>> cont)
        {
            return CloudBuilder.Bind(workflow, (tuple => cont.Invoke(tuple.Item1, tuple.Item2, tuple.Item3)));
        }

        /// <summary>
        ///     Binds result of provided workflow to supplied continuation lambda.
        /// </summary>
        /// <typeparam name="T1"></typeparam>
        /// <typeparam name="T2"></typeparam>
        /// <typeparam name="T3"></typeparam>
        /// <typeparam name="T4"></typeparam>
        /// <typeparam name="S"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="cont">Result continuation.</param>
        /// <returns>A cloud workflow composed of the two.</returns>
        public static Cloud<S> Bind<T1, T2, T3, T4, S>(this Cloud<Tuple<T1, T2, T3, T4>> workflow, Func<T1, T2, T3, T4, Cloud<S>> cont)
        {
            return CloudBuilder.Bind(workflow, (tuple => cont.Invoke(tuple.Item1, tuple.Item2, tuple.Item3, tuple.Item4)));
        }

        /// <summary>
        ///     Binds result of provided workflow to supplied continuation lambda.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="S"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="cont">Result continuation.</param>
        /// <returns>A cloud workflow composed of the two.</returns>
        public static LocalCloud<S> Bind<T, S>(this LocalCloud<T> workflow, Func<T, LocalCloud<S>> cont)
        {
            return Builders.local.Bind(workflow, FSharpFunc.Create(cont));
        }

        /// <summary>
        ///     Binds result of provided workflow to supplied continuation lambda.
        /// </summary>
        /// <typeparam name="S"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="cont">Result continuation.</param>
        /// <returns>A cloud workflow composed of the two.</returns>
        public static LocalCloud<S> Bind<S>(this LocalCloud<unit> workflow, Func<LocalCloud<S>> cont)
        {
            return Builders.local.Bind(workflow, FSharpFunc.Create(cont));
        }

        /// <summary>
        ///     Sequentually composes collection of supplied workflows, returning a tuple containing the results.
        /// </summary>
        /// <typeparam name="T1"></typeparam>
        /// <typeparam name="T2"></typeparam>
        /// <typeparam name="S"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="cont">Result continuation.</param>
        /// <returns>A cloud workflow composed of the two.</returns>
        public static LocalCloud<S> Bind<T1, T2, S>(this LocalCloud<Tuple<T1, T2>> workflow, Func<T1, T2, LocalCloud<S>> cont)
        {
            return CloudBuilder.Bind(workflow, (tuple => cont.Invoke(tuple.Item1, tuple.Item2)));
        }

        /// <summary>
        ///     Binds result of provided workflow to supplied continuation lambda.
        /// </summary>
        /// <typeparam name="T1"></typeparam>
        /// <typeparam name="T2"></typeparam>
        /// <typeparam name="T3"></typeparam>
        /// <typeparam name="S"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="cont">Result continuation.</param>
        /// <returns>A cloud workflow composed of the two.</returns>
        public static LocalCloud<S> Bind<T1, T2, T3, S>(this LocalCloud<Tuple<T1, T2, T3>> workflow, Func<T1, T2, T3, LocalCloud<S>> cont)
        {
            return CloudBuilder.Bind(workflow, (tuple => cont.Invoke(tuple.Item1, tuple.Item2, tuple.Item3)));
        }

        /// <summary>
        ///     Binds result of provided workflow to supplied continuation lambda.
        /// </summary>
        /// <typeparam name="T1"></typeparam>
        /// <typeparam name="T2"></typeparam>
        /// <typeparam name="T3"></typeparam>
        /// <typeparam name="T4"></typeparam>
        /// <typeparam name="S"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="cont">Result continuation.</param>
        /// <returns>A cloud workflow composed of the two.</returns>
        public static LocalCloud<S> Bind<T1, T2, T3, T4, S>(this LocalCloud<Tuple<T1, T2, T3, T4>> workflow, Func<T1, T2, T3, T4, LocalCloud<S>> cont)
        {
            return CloudBuilder.Bind(workflow, (tuple => cont.Invoke(tuple.Item1, tuple.Item2, tuple.Item3, tuple.Item4)));
        }

        /// <summary>
        ///     Sequentually composes collection of supplied workflows, returning a tuple containing the results.
        /// </summary>
        /// <typeparam name="T1">First return type.</typeparam>
        /// <typeparam name="T2">Second return type.</typeparam>
        /// <param name="workflow1">First workflow.</param>
        /// <param name="workflow2">Second workflow.</param>
        /// <returns>A composed cloud workflow returning a tuple of results.</returns>
        public static Cloud<Tuple<T1, T2>> Combine<T1, T2>(this Cloud<T1> workflow1, Cloud<T2> workflow2)
        {
            return workflow1.Bind(t1 => workflow2.OnSuccess(t2 => new Tuple<T1, T2>(t1, t2)));
        }

        /// <summary>
        ///     Sequentually composes collection of supplied workflows, returning a tuple containing the results.
        /// </summary>
        /// <typeparam name="T1">First return type.</typeparam>
        /// <typeparam name="T2">Second return type.</typeparam>
        /// <typeparam name="T3">Third return type.</typeparam>
        /// <param name="workflow1">This workflow.</param>
        /// <param name="workflow2">Second workflow.</param>
        /// <param name="workflow3">Third workflow.</param>
        /// <returns>A composed cloud workflow returning a tuple of results.</returns>
        public static Cloud<Tuple<T1, T2, T3>> Combine<T1, T2, T3>(this Cloud<T1> workflow1, Cloud<T2> workflow2, Cloud<T3> workflow3)
        {
            return workflow1.Bind(t1 => workflow2.Bind(t2 => workflow3.OnSuccess(t3 => new Tuple<T1, T2, T3>(t1, t2, t3))));
        }

        /// <summary>
        ///     Binds result of provided workflow to supplied continuation lambda.
        /// </summary>
        /// <typeparam name="T1">First return type.</typeparam>
        /// <typeparam name="T2">Second return type.</typeparam>
        /// <typeparam name="T3">Third return type.</typeparam>
        /// <typeparam name="T4">Fourth return type.</typeparam>
        /// <param name="workflow1">This workflow.</param>
        /// <param name="workflow2">Second workflow.</param>
        /// <param name="workflow3">Third workflow.</param>
        /// <param name="workflow4">Fourth workflow.</param>
        /// <returns>A composed cloud workflow returning a tuple of results.</returns>
        public static Cloud<Tuple<T1, T2, T3, T4>> Combine<T1, T2, T3, T4>(this Cloud<T1> workflow1, Cloud<T2> workflow2, Cloud<T3> workflow3, Cloud<T4> workflow4)
        {
            return workflow1.Bind(t1 => workflow2.Bind(t2 => workflow3.Bind(t3 => workflow4.OnSuccess(t4 => new Tuple<T1, T2, T3, T4>(t1, t2, t3, t4)))));
        }

        /// <summary>
        ///      Sequentually composes collection of supplied workflows, returning a tuple containing the results.
        /// </summary>
        /// <typeparam name="T1">First return type.</typeparam>
        /// <typeparam name="T2">Second return type.</typeparam>
        /// <param name="workflow1">This workflow.</param>
        /// <param name="workflow2">Second workflow.</param>
        /// <returns>A composed cloud workflow returning a tuple of results.</returns>
        public static LocalCloud<Tuple<T1,T2>> Combine<T1,T2>(this LocalCloud<T1> workflow1, LocalCloud<T2> workflow2)
        {
            return workflow1.Bind(t1 => workflow2.OnSuccess(t2 => new Tuple<T1, T2>(t1, t2)));
        }

        /// <summary>
        ///     Sequentually composes collection of supplied workflows, returning a tuple containing the results.
        /// </summary>
        /// <typeparam name="T1">First return type.</typeparam>
        /// <typeparam name="T2">Second return type.</typeparam>
        /// <typeparam name="T3">Third return type.</typeparam>
        /// <param name="workflow1">This workflow.</param>
        /// <param name="workflow2">Second workflow.</param>
        /// <param name="workflow3">Third workflow.</param>
        /// <returns>A composed cloud workflow returning a tuple of results.</returns>
        public static LocalCloud<Tuple<T1, T2, T3>> Combine<T1, T2, T3>(this LocalCloud<T1> workflow1, LocalCloud<T2> workflow2, LocalCloud<T3> workflow3)
        {
            return workflow1.Bind(t1 => workflow2.Bind(t2 => workflow3.OnSuccess(t3 => new Tuple<T1, T2, T3>(t1, t2, t3))));
        }

        /// <summary>
        ///     Binds result of provided workflow to supplied continuation lambda.
        /// </summary>
        /// <typeparam name="T1">First return type.</typeparam>
        /// <typeparam name="T2">Second return type.</typeparam>
        /// <typeparam name="T3">Third return type.</typeparam>
        /// <typeparam name="T4">Fourth return type.</typeparam>
        /// <param name="workflow1">This workflow.</param>
        /// <param name="workflow2">Second workflow.</param>
        /// <param name="workflow3">Third workflow.</param>
        /// <param name="workflow4">Fourth workflow.</param>
        /// <returns>A composed cloud workflow returning a tuple of results.</returns>
        public static LocalCloud<Tuple<T1, T2, T3, T4>> Combine<T1, T2, T3, T4>(this LocalCloud<T1> workflow1, LocalCloud<T2> workflow2, LocalCloud<T3> workflow3, LocalCloud<T4> workflow4)
        {
            return workflow1.Bind(t1 => workflow2.Bind(t2 => workflow3.Bind(t3 => workflow4.OnSuccess(t4 => new Tuple<T1, T2, T3, T4>(t1, t2, t3, t4)))));
        }

        /// <summary>
        ///     Catches exception with supplied handler lambda.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="handler">Exception handler lambda.</param>
        /// <returns>A cloud workflow that catches workflow using compensation lambda.</returns>
        public static Cloud<T> Catch<T>(this Cloud<T> workflow, Func<Exception, Cloud<T>> handler)
        {
            var c = Builders.cloud;
            return c.TryWith(workflow, FSharpFunc.Create(handler));
        }

        /// <summary>
        ///     Catches exception with of given type with supplied handler lambda.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="Exception">Exception type to match against.</typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="handler">Exception handler lambda.</param>
        /// <returns>A cloud workflow that catches workflow using compensation lambda.</returns>
        public static Cloud<T> Catch<T, Exception>(this Cloud<T> workflow, Func<Exception, Cloud<T>> handler) where Exception : System.Exception
        {
            return CloudBuilder.Catch<T>(workflow, (exn =>
            {
                if (exn is Exception)
                    return handler.Invoke((Exception)exn);

                return CloudBuilder.Throw<T>(exn);
            }));
        }

        /// <summary>
        ///     Catches exception with supplied handler lambda.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="handler">Exception handler lambda.</param>
        /// <returns>A cloud workflow that catches workflow using compensation lambda.</returns>
        public static LocalCloud<T> Catch<T>(this LocalCloud<T> workflow, Func<Exception, LocalCloud<T>> handler)
        {
            var l = Builders.local;
            return l.TryWith(workflow, FSharpFunc.Create(handler));
        }


        /// <summary>
        ///     Catches exception with supplied handler lambda.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="Exception"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="handler">Exception handler lambda.</param>
        /// <returns>A cloud workflow that catches workflow using compensation lambda.</returns>
        public static LocalCloud<T> Catch<T, Exception>(this LocalCloud<T> workflow, Func<Exception, LocalCloud<T>> handler) where Exception : System.Exception
        {
            return CloudBuilder.Catch<T>(workflow, (exn =>
            {
                if (exn is Exception)
                    return handler.Invoke((Exception)exn);

                return CloudBuilder.Throw<T>(exn);
            }));
        }

        /// <summary>
        ///     Binds result of provided workflow to supplied continuation lambda.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="S"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="cont">Result continuation.</param>
        /// <returns>A cloud workflow composed of the two.</returns>
        public static Cloud<S> OnSuccess<T,S>(this Cloud<T> workflow, Func<T,S> cont)
        {
            return CloudBuilder.Bind(workflow, CloudBuilder.FromFunc(cont));
        }

        /// <summary>
        ///     Binds result of provided workflow to supplied continuation lambda.
        /// </summary>
        /// <typeparam name="S"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="cont">Result continuation.</param>
        /// <returns>A cloud workflow composed of the two.</returns>
        public static Cloud<S> OnSuccess<S>(this Cloud<unit> workflow, Func<S> cont)
        {
            return CloudBuilder.Bind(workflow, CloudBuilder.FromFunc((unit u) => cont.Invoke()));
        }

        /// <summary>
        ///     Binds result of provided workflow to supplied continuation lambda.
        /// </summary>
        /// <typeparam name="T1"></typeparam>
        /// <typeparam name="T2"></typeparam>
        /// <typeparam name="S"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="cont">Result continuation.</param>
        /// <returns>A cloud workflow composed of the two.</returns>
        public static Cloud<S> OnSuccess<T1, T2, S>(this Cloud<Tuple<T1,T2>> workflow, Func<T1, T2, S> cont)
        {
            return CloudBuilder.OnSuccess(workflow, (tuple => cont.Invoke(tuple.Item1, tuple.Item2)));
        }

        /// <summary>
        ///     Binds result of provided workflow to supplied continuation lambda.
        /// </summary>
        /// <typeparam name="T1"></typeparam>
        /// <typeparam name="T2"></typeparam>
        /// <typeparam name="T3"></typeparam>
        /// <typeparam name="S"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="cont">Result continuation.</param>
        /// <returns>A cloud workflow composed of the two.</returns>
        public static Cloud<S> OnSuccess<T1, T2, T3, S>(this Cloud<Tuple<T1, T2, T3>> workflow, Func<T1, T2, T3, S> cont)
        {
            return CloudBuilder.OnSuccess(workflow, (tuple => cont.Invoke(tuple.Item1, tuple.Item2, tuple.Item3)));
        }

        /// <summary>
        ///     Binds result of provided workflow to supplied continuation lambda.
        /// </summary>
        /// <typeparam name="T1"></typeparam>
        /// <typeparam name="T2"></typeparam>
        /// <typeparam name="T3"></typeparam>
        /// <typeparam name="T4"></typeparam>
        /// <typeparam name="S"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="cont">Result continuation.</param>
        /// <returns>A cloud workflow composed of the two.</returns>
        public static Cloud<S> OnSuccess<T1, T2, T3, T4, S>(this Cloud<Tuple<T1, T2, T3, T4>> workflow, Func<T1, T2, T3, T4, S> cont)
        {
            return CloudBuilder.OnSuccess(workflow, (tuple => cont.Invoke(tuple.Item1, tuple.Item2, tuple.Item3, tuple.Item4)));
        }

        /// <summary>
        ///     Binds result of provided workflow to supplied continuation lambda.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="S"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="cont">Result continuation.</param>
        /// <returns>A cloud workflow composed of the two.</returns>
        public static LocalCloud<S> OnSuccess<T, S>(this LocalCloud<T> workflow, Func<T, S> cont)
        {
            return CloudBuilder.Bind(workflow, CloudBuilder.FromFunc(cont));
        }

        /// <summary>
        ///     Binds result of provided workflow to supplied continuation lambda.
        /// </summary>
        /// <typeparam name="S"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="cont">Result continuation.</param>
        /// <returns>A cloud workflow composed of the two.</returns>
        public static LocalCloud<S> OnSuccess<S>(this LocalCloud<unit> workflow, Func<S> cont)
        {
            return CloudBuilder.Bind(workflow, CloudBuilder.FromFunc((unit u) => cont.Invoke()));
        }

        /// <summary>
        ///     Binds result of provided workflow to supplied continuation lambda.
        /// </summary>
        /// <typeparam name="T1"></typeparam>
        /// <typeparam name="T2"></typeparam>
        /// <typeparam name="S"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="cont">Result continuation.</param>
        /// <returns>A cloud workflow composed of the two.</returns>
        public static LocalCloud<S> OnSuccess<T1, T2, S>(this LocalCloud<Tuple<T1, T2>> workflow, Func<T1, T2, S> cont)
        {
            return CloudBuilder.OnSuccess(workflow, (tuple => cont.Invoke(tuple.Item1, tuple.Item2)));
        }

        /// <summary>
        ///     Binds result of provided workflow to supplied continuation lambda.
        /// </summary>
        /// <typeparam name="T1"></typeparam>
        /// <typeparam name="T2"></typeparam>
        /// <typeparam name="T3"></typeparam>
        /// <typeparam name="S"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="cont">Result continuation.</param>
        /// <returns>A cloud workflow composed of the two.</returns>
        public static LocalCloud<S> OnSuccess<T1, T2, T3, S>(this LocalCloud<Tuple<T1, T2, T3>> workflow, Func<T1, T2, T3, S> cont)
        {
            return CloudBuilder.OnSuccess(workflow, (tuple => cont.Invoke(tuple.Item1, tuple.Item2, tuple.Item3)));
        }

        /// <summary>
        ///     Binds result of provided workflow to supplied continuation lambda.
        /// </summary>
        /// <typeparam name="T1"></typeparam>
        /// <typeparam name="T2"></typeparam>
        /// <typeparam name="T3"></typeparam>
        /// <typeparam name="T4"></typeparam>
        /// <typeparam name="S"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="cont">Result continuation.</param>
        /// <returns>A cloud workflow composed of the two.</returns>
        public static LocalCloud<S> OnSuccess<T1, T2, T3, T4, S>(this LocalCloud<Tuple<T1, T2, T3, T4>> workflow, Func<T1, T2, T3, T4, S> cont)
        {
            return CloudBuilder.OnSuccess(workflow, (tuple => cont.Invoke(tuple.Item1, tuple.Item2, tuple.Item3, tuple.Item4)));
        }

        /// <summary>
        ///     Binds result of provided workflow to supplied continuation lambda.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="cont">Result continuation.</param>
        /// <returns>A cloud workflow composed of the two.</returns>
        public static Cloud<Unit> OnSuccess<T>(this Cloud<T> workflow, Action<T> cont)
        {
            return CloudBuilder.Bind(workflow, CloudBuilder.FromFunc(cont));
        }

        /// <summary>
        ///     Binds result of provided workflow to supplied continuation lambda.
        /// </summary>
        /// <typeparam name="T1"></typeparam>
        /// <typeparam name="T2"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="cont">Result continuation.</param>
        /// <returns>A cloud workflow composed of the two.</returns>
        public static Cloud<Unit> OnSuccess<T1,T2>(this Cloud<Tuple<T1,T2>> workflow, Action<T1,T2> cont)
        {
            return CloudBuilder.OnSuccess(workflow, (tuple => cont.Invoke(tuple.Item1, tuple.Item2)));
        }

        /// <summary>
        ///     Binds result of provided workflow to supplied continuation lambda.
        /// </summary>
        /// <typeparam name="T1"></typeparam>
        /// <typeparam name="T2"></typeparam>
        /// <typeparam name="T3"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="cont">Result continuation.</param>
        /// <returns>A cloud workflow composed of the two.</returns>
        public static Cloud<Unit> OnSuccess<T1, T2, T3>(this Cloud<Tuple<T1, T2, T3>> workflow, Action<T1, T2, T3> cont)
        {
            return CloudBuilder.OnSuccess(workflow, (tuple => cont.Invoke(tuple.Item1, tuple.Item2, tuple.Item3)));
        }

        /// <summary>
        ///     Binds result of provided workflow to supplied continuation lambda.
        /// </summary>
        /// <typeparam name="T1"></typeparam>
        /// <typeparam name="T2"></typeparam>
        /// <typeparam name="T3"></typeparam>
        /// <typeparam name="T4"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="cont">Result continuation.</param>
        /// <returns>A cloud workflow composed of the two.</returns>
        public static Cloud<Unit> OnSuccess<T1, T2, T3, T4>(this Cloud<Tuple<T1, T2, T3, T4>> workflow, Action<T1, T2, T3, T4> cont)
        {
            return CloudBuilder.OnSuccess(workflow, (tuple => cont.Invoke(tuple.Item1, tuple.Item2, tuple.Item3, tuple.Item4)));
        }

        /// <summary>
        ///     Binds result of provided workflow to supplied continuation lambda.
        /// </summary>
        /// <param name="workflow">This workflow.</param>
        /// <param name="cont">Result continuation.</param>
        /// <returns>A cloud workflow composed of the two.</returns>
        public static Cloud<Unit> OnSuccess(this Cloud<Unit> workflow, Action cont)
        {
            return CloudBuilder.Bind(workflow, CloudBuilder.FromFunc((Unit u) => cont.Invoke()));
        }

        /// <summary>
        ///     Binds result of provided workflow to supplied continuation lambda.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="cont">Result continuation.</param>
        /// <returns>A cloud workflow composed of the two.</returns>
        public static LocalCloud<Unit> OnSuccess<T>(this LocalCloud<T> workflow, Action<T> cont)
        {
            return CloudBuilder.Bind(workflow, CloudBuilder.FromFunc(cont));
        }

        /// <summary>
        ///     Binds result of provided workflow to supplied continuation lambda.
        /// </summary>
        /// <typeparam name="T1"></typeparam>
        /// <typeparam name="T2"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="cont">Result continuation.</param>
        /// <returns>A cloud workflow composed of the two.</returns>
        public static LocalCloud<Unit> OnSuccess<T1, T2>(this LocalCloud<Tuple<T1, T2>> workflow, Action<T1, T2> cont)
        {
            return CloudBuilder.OnSuccess(workflow, (tuple => cont.Invoke(tuple.Item1, tuple.Item2)));
        }

        /// <summary>
        ///     Binds result of provided workflow to supplied continuation lambda.
        /// </summary>
        /// <typeparam name="T1"></typeparam>
        /// <typeparam name="T2"></typeparam>
        /// <typeparam name="T3"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="cont">Result continuation.</param>
        /// <returns>A cloud workflow composed of the two.</returns>
        public static LocalCloud<Unit> OnSuccess<T1, T2, T3>(this LocalCloud<Tuple<T1, T2, T3>> workflow, Action<T1, T2, T3> cont)
        {
            return CloudBuilder.OnSuccess(workflow, (tuple => cont.Invoke(tuple.Item1, tuple.Item2, tuple.Item3)));
        }

        /// <summary>
        ///     Binds result of provided workflow to supplied continuation lambda.
        /// </summary>
        /// <typeparam name="T1"></typeparam>
        /// <typeparam name="T2"></typeparam>
        /// <typeparam name="T3"></typeparam>
        /// <typeparam name="T4"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="cont">Result continuation.</param>
        /// <returns>A cloud workflow composed of the two.</returns>
        public static LocalCloud<Unit> OnSuccess<T1, T2, T3, T4>(this LocalCloud<Tuple<T1, T2, T3, T4>> workflow, Action<T1, T2, T3, T4> cont)
        {
            return CloudBuilder.OnSuccess(workflow, (tuple => cont.Invoke(tuple.Item1, tuple.Item2, tuple.Item3, tuple.Item4)));
        }

        /// <summary>
        ///     Binds result of provided workflow to supplied continuation lambda.
        /// </summary>
        /// <param name="workflow">This workflow.</param>
        /// <param name="cont">Result continuation.</param>
        /// <returns>A cloud workflow composed of the two.</returns>
        public static LocalCloud<Unit> OnSuccess(this LocalCloud<unit> workflow, Action cont)
        {
            return CloudBuilder.Bind(workflow, CloudBuilder.FromFunc((Unit u) => cont.Invoke()));
        }

        /// <summary>
        ///     Catches exception with supplied handler lambda.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="handler">Exception handler lambda.</param>
        /// <returns>A cloud workflow that catches workflow using compensation lambda.</returns>
        public static Cloud<T> OnFailure<T>(this Cloud<T> workflow, Func<Exception, T> handler)
        {
            var f = (Func<Exception, Cloud<T>>) CloudBuilder.FromFunc(handler);
            return CloudBuilder.Catch(workflow, f);
        }

        /// <summary>
        ///     Catches exception of given type with supplied handler lambda.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="Exception">Exception type to catch</typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="handler">Exception handler lambda.</param>
        /// <returns>A cloud workflow that catches workflow using compensation lambda.</returns>
        public static Cloud<T> OnFailure<T, Exception>(this Cloud<T> workflow, Func<Exception, T> handler) where Exception : System.Exception
        {
            var f = (Func<Exception, Cloud<T>>)CloudBuilder.FromFunc(handler);
            return CloudBuilder.Catch<T, Exception>(workflow, f);
        }

        /// <summary>
        ///     Catches exception with supplied handler lambda.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="handler">Exception handler lambda.</param>
        /// <returns>A cloud workflow that catches workflow using compensation lambda.</returns>
        public static LocalCloud<T> OnFailure<T>(this LocalCloud<T> workflow, Func<Exception, T> handler)
        {
            return CloudBuilder.Catch(workflow, CloudBuilder.FromFunc(handler));
        }

        /// <summary>
        ///     Catches exception of given type with supplied handler lambda.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="Exception">Exception type to catch</typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="handler">Exception handler lambda.</param>
        /// <returns>A cloud workflow that catches workflow using compensation lambda.</returns>
        public static LocalCloud<T> OnFailure<T, Exception>(this LocalCloud<T> workflow, Func<Exception, T> handler) where Exception : System.Exception
        {
            var f = (Func<Exception, LocalCloud<T>>)CloudBuilder.FromFunc(handler);
            return CloudBuilder.Catch<T, Exception>(workflow, f);
        }

        /// <summary>
        ///     Specifies set of actions to perform on completion of a cloud workflow.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="S"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="onSuccess">Action to perform on success of cloud workflow.</param>
        /// <param name="onFailure">Action to perform on failure of cloud workflow.</param>
        /// <returns></returns>
        public static Cloud<S> OnComplete<T,S>(this Cloud<T> workflow, Func<T,S> onSuccess, Func<Exception,S> onFailure)
        {
            var protectedW = Cloud.Catch(workflow);
            return protectedW.OnSuccess<FSharpChoice<T, Exception>, S>(result =>
                {
                    if (result is FSharpChoice<T, Exception>.Choice1Of2)
                    {
                        var c = (FSharpChoice<T, Exception>.Choice1Of2)result;
                        return onSuccess.Invoke(c.Item);
                    }
                    else
                    {
                        var c = (FSharpChoice<T, Exception>.Choice2Of2)result;
                        return onFailure.Invoke(c.Item);
                    }

                });
        }

        /// <summary>
        ///     Specifies set of actions to perform on completion of a cloud workflow.
        /// </summary>
        /// <typeparam name="S"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="onSuccess">Action to perform on success of cloud workflow.</param>
        /// <param name="onFailure">Action to perform on failure of cloud workflow.</param>
        /// <returns></returns>
        public static Cloud<S> OnComplete<S>(this Cloud<unit> workflow, Func<S> onSuccess, Func<Exception, S> onFailure)
        {
            return workflow.OnComplete(((Unit u) => onSuccess.Invoke()), onFailure);
        }

        /// <summary>
        ///     Specifies set of actions to perform on completion of a cloud workflow.
        /// </summary>
        /// <typeparam name="T1"></typeparam>
        /// <typeparam name="T2"></typeparam>
        /// <typeparam name="S"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="onSuccess">Action to perform on success of cloud workflow.</param>
        /// <param name="onFailure">Action to perform on failure of cloud workflow.</param>
        /// <returns></returns>
        public static Cloud<S> OnComplete<T1,T2, S>(this Cloud<Tuple<T1,T2>> workflow, Func<T1,T2, S> onSuccess, Func<Exception, S> onFailure)
        {
            return CloudBuilder.OnComplete<Tuple<T1, T2>, S>(workflow, (t => onSuccess.Invoke(t.Item1, t.Item2)), onFailure);
        }

        /// <summary>
        ///     Specifies set of actions to perform on completion of a cloud workflow.
        /// </summary>
        /// <typeparam name="T1"></typeparam>
        /// <typeparam name="T2"></typeparam>
        /// <typeparam name="T3"></typeparam>
        /// <typeparam name="S"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="onSuccess">Action to perform on success of cloud workflow.</param>
        /// <param name="onFailure">Action to perform on failure of cloud workflow.</param>
        /// <returns></returns>
        public static Cloud<S> OnComplete<T1, T2, T3, S>(this Cloud<Tuple<T1, T2, T3>> workflow, Func<T1, T2, T3, S> onSuccess, Func<Exception, S> onFailure)
        {
            return CloudBuilder.OnComplete<Tuple<T1, T2, T3>, S>(workflow, (t => onSuccess.Invoke(t.Item1, t.Item2, t.Item3)), onFailure);
        }

        /// <summary>
        ///     Specifies set of actions to perform on completion of a cloud workflow.
        /// </summary>
        /// <typeparam name="T1"></typeparam>
        /// <typeparam name="T2"></typeparam>
        /// <typeparam name="T3"></typeparam>
        /// <typeparam name="T4"></typeparam>
        /// <typeparam name="S"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="onSuccess">Action to perform on success of cloud workflow.</param>
        /// <param name="onFailure">Action to perform on failure of cloud workflow.</param>
        /// <returns></returns>
        public static Cloud<S> OnComplete<T1, T2, T3, T4, S>(this Cloud<Tuple<T1, T2, T3, T4>> workflow, Func<T1, T2, T3, T4, S> onSuccess, Func<Exception, S> onFailure)
        {
            return CloudBuilder.OnComplete<Tuple<T1, T2, T3, T4>, S>(workflow, (t => onSuccess.Invoke(t.Item1, t.Item2, t.Item3, t.Item4)), onFailure);
        }

        /// <summary>
        ///     Specifies set of actions to perform on completion of a cloud workflow.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="S"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="onSuccess">Action to perform on success of cloud workflow.</param>
        /// <param name="onFailure">Action to perform on failure of cloud workflow.</param>
        /// <returns></returns>
        public static LocalCloud<S> OnComplete<T, S>(this LocalCloud<T> workflow, Func<T, S> onSuccess, Func<Exception, S> onFailure)
        {
            var protectedW = Local.Catch(workflow);
            return protectedW.OnSuccess<FSharpChoice<T, Exception>, S>(result =>
            {
                if (result is FSharpChoice<T, Exception>.Choice1Of2)
                {
                    var c = (FSharpChoice<T, Exception>.Choice1Of2)result;
                    return onSuccess.Invoke(c.Item);
                }
                else
                {
                    var c = (FSharpChoice<T, Exception>.Choice2Of2)result;
                    return onFailure.Invoke(c.Item);
                }

            });
        }

        /// <summary>
        ///     Specifies set of actions to perform on completion of a cloud workflow.
        /// </summary>
        /// <typeparam name="S"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="onSuccess">Action to perform on success of cloud workflow.</param>
        /// <param name="onFailure">Action to perform on failure of cloud workflow.</param>
        /// <returns></returns>
        public static LocalCloud<S> OnComplete<S>(this LocalCloud<unit> workflow, Func<S> onSuccess, Func<Exception, S> onFailure)
        {
            return workflow.OnComplete(((Unit u) => onSuccess.Invoke()), onFailure);
        }

        /// <summary>
        ///     Specifies set of actions to perform on completion of a cloud workflow.
        /// </summary>
        /// <typeparam name="T1"></typeparam>
        /// <typeparam name="T2"></typeparam>
        /// <typeparam name="S"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="onSuccess">Action to perform on success of cloud workflow.</param>
        /// <param name="onFailure">Action to perform on failure of cloud workflow.</param>
        /// <returns></returns>
        public static LocalCloud<S> OnComplete<T1, T2, S>(this LocalCloud<Tuple<T1, T2>> workflow, Func<T1, T2, S> onSuccess, Func<Exception, S> onFailure)
        {
            return CloudBuilder.OnComplete<Tuple<T1, T2>, S>(workflow, (t => onSuccess.Invoke(t.Item1, t.Item2)), onFailure);
        }

        /// <summary>
        ///     Specifies set of actions to perform on completion of a cloud workflow.
        /// </summary>
        /// <typeparam name="T1"></typeparam>
        /// <typeparam name="T2"></typeparam>
        /// <typeparam name="T3"></typeparam>
        /// <typeparam name="S"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="onSuccess">Action to perform on success of cloud workflow.</param>
        /// <param name="onFailure">Action to perform on failure of cloud workflow.</param>
        /// <returns></returns>
        public static LocalCloud<S> OnComplete<T1, T2, T3, S>(this LocalCloud<Tuple<T1, T2, T3>> workflow, Func<T1, T2, T3, S> onSuccess, Func<Exception, S> onFailure)
        {
            return CloudBuilder.OnComplete<Tuple<T1, T2, T3>, S>(workflow, (t => onSuccess.Invoke(t.Item1, t.Item2, t.Item3)), onFailure);
        }

        /// <summary>
        ///     Specifies set of actions to perform on completion of a cloud workflow.
        /// </summary>
        /// <typeparam name="T1"></typeparam>
        /// <typeparam name="T2"></typeparam>
        /// <typeparam name="T3"></typeparam>
        /// <typeparam name="T4"></typeparam>
        /// <typeparam name="S"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="onSuccess">Action to perform on success of cloud workflow.</param>
        /// <param name="onFailure">Action to perform on failure of cloud workflow.</param>
        /// <returns></returns>
        public static LocalCloud<S> OnComplete<T1, T2, T3, T4, S>(this LocalCloud<Tuple<T1, T2, T3, T4>> workflow, Func<T1, T2, T3, T4, S> onSuccess, Func<Exception, S> onFailure)
        {
            return CloudBuilder.OnComplete<Tuple<T1, T2, T3, T4>, S>(workflow, (t => onSuccess.Invoke(t.Item1, t.Item2, t.Item3, t.Item4)), onFailure);
        }

        /// <summary>
        ///     Specifies set of actions to perform on completion of a cloud workflow.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="onSuccess">Action to perform on success of cloud workflow.</param>
        /// <param name="onFailure">Action to perform on failure of cloud workflow.</param>
        /// <returns></returns>
        public static Cloud<Unit> OnComplete<T>(this Cloud<T> workflow, Action<T> onSuccess, Action<Exception> onFailure)
        {
            var protectedW = Cloud.Catch(workflow);
            return protectedW.OnSuccess<FSharpChoice<T, Exception>, Unit>(result =>
            {
                if (result is FSharpChoice<T, Exception>.Choice1Of2)
                {
                    var c = (FSharpChoice<T, Exception>.Choice1Of2)result;
                    onSuccess.Invoke(c.Item);
                }
                else
                {
                    var c = (FSharpChoice<T, Exception>.Choice2Of2)result;
                    onFailure.Invoke(c.Item);
                }

                return null;
            });
        }

        /// <summary>
        ///     Specifies set of actions to perform on completion of a cloud workflow.
        /// </summary>
        /// <typeparam name="T1"></typeparam>
        /// <typeparam name="T2"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="onSuccess">Action to perform on success of cloud workflow.</param>
        /// <param name="onFailure">Action to perform on failure of cloud workflow.</param>
        /// <returns></returns>
        public static Cloud<Unit> OnComplete<T1, T2>(this Cloud<Tuple<T1, T2>> workflow, Action<T1, T2> onSuccess, Action<Exception> onFailure)
        {
            return CloudBuilder.OnComplete<Tuple<T1, T2>>(workflow, (tuple => onSuccess.Invoke(tuple.Item1, tuple.Item2)), onFailure);
        }

        /// <summary>
        ///     Specifies set of actions to perform on completion of a cloud workflow.
        /// </summary>
        /// <typeparam name="T1"></typeparam>
        /// <typeparam name="T2"></typeparam>
        /// <typeparam name="T3"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="onSuccess">Action to perform on success of cloud workflow.</param>
        /// <param name="onFailure">Action to perform on failure of cloud workflow.</param>
        /// <returns></returns>
        public static Cloud<Unit> OnComplete<T1, T2, T3>(this Cloud<Tuple<T1, T2, T3>> workflow, Action<T1, T2, T3> onSuccess, Action<Exception> onFailure)
        {
            return CloudBuilder.OnComplete<Tuple<T1, T2, T3>>(workflow, (tuple => onSuccess.Invoke(tuple.Item1, tuple.Item2, tuple.Item3)), onFailure);
        }

        /// <summary>
        ///     Specifies set of actions to perform on completion of a cloud workflow.
        /// </summary>
        /// <typeparam name="T1"></typeparam>
        /// <typeparam name="T2"></typeparam>
        /// <typeparam name="T3"></typeparam>
        /// <typeparam name="T4"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="onSuccess">Action to perform on success of cloud workflow.</param>
        /// <param name="onFailure">Action to perform on failure of cloud workflow.</param>
        /// <returns></returns>
        public static Cloud<Unit> OnComplete<T1, T2, T3, T4>(this Cloud<Tuple<T1, T2, T3, T4>> workflow, Action<T1, T2, T3, T4> onSuccess, Action<Exception> onFailure)
        {
            return CloudBuilder.OnComplete<Tuple<T1, T2, T3, T4>>(workflow, (tuple => onSuccess.Invoke(tuple.Item1, tuple.Item2, tuple.Item3, tuple.Item4)), onFailure);
        }

        /// <summary>
        ///     Specifies set of actions to perform on completion of a cloud workflow.
        /// </summary>
        /// <param name="workflow">This workflow.</param>
        /// <param name="onSuccess">Action to perform on success of cloud workflow.</param>
        /// <param name="onFailure">Action to perform on failure of cloud workflow.</param>
        /// <returns></returns>
        public static Cloud<Unit> OnComplete(this Cloud<unit> workflow, Action onSuccess, Action<Exception> onFailure)
        {
            return workflow.OnComplete(((Unit u) => onSuccess.Invoke()), onFailure);
        }

        /// <summary>
        ///     Specifies set of actions to perform on completion of a cloud workflow.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="onSuccess">Action to perform on success of cloud workflow.</param>
        /// <param name="onFailure">Action to perform on failure of cloud workflow.</param>
        /// <returns></returns>
        public static LocalCloud<Unit> OnComplete<T>(this LocalCloud<T> workflow, Action<T> onSuccess, Action<Exception> onFailure)
        {
            var protectedW = Local.Catch(workflow);
            return protectedW.OnSuccess<FSharpChoice<T, Exception>, Unit>(result =>
            {
                if (result is FSharpChoice<T, Exception>.Choice1Of2)
                {
                    var c = (FSharpChoice<T, Exception>.Choice1Of2)result;
                    onSuccess.Invoke(c.Item);
                }
                else
                {
                    var c = (FSharpChoice<T, Exception>.Choice2Of2)result;
                    onFailure.Invoke(c.Item);
                }

                return null;
            });
        }

        /// <summary>
        ///     Specifies set of actions to perform on completion of a cloud workflow.
        /// </summary>
        /// <param name="workflow">This workflow.</param>
        /// <param name="onSuccess">Action to perform on success of cloud workflow.</param>
        /// <param name="onFailure">Action to perform on failure of cloud workflow.</param>
        /// <returns></returns>
        public static LocalCloud<Unit> OnComplete(this LocalCloud<unit> workflow, Action onSuccess, Action<Exception> onFailure)
        {
            return workflow.OnComplete(((Unit u) => onSuccess.Invoke()), onFailure);
        }

        /// <summary>
        ///     Specifies set of actions to perform on completion of a cloud workflow.
        /// </summary>
        /// <typeparam name="T1"></typeparam>
        /// <typeparam name="T2"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="onSuccess">Action to perform on success of cloud workflow.</param>
        /// <param name="onFailure">Action to perform on failure of cloud workflow.</param>
        /// <returns></returns>
        public static LocalCloud<Unit> OnComplete<T1,T2>(this LocalCloud<Tuple<T1, T2>> workflow, Action<T1,T2> onSuccess, Action<Exception> onFailure)
        {
            return CloudBuilder.OnComplete<Tuple<T1, T2>>(workflow, (tuple => onSuccess.Invoke(tuple.Item1, tuple.Item2)), onFailure);
        }

        /// <summary>
        ///     Specifies set of actions to perform on completion of a cloud workflow.
        /// </summary>
        /// <typeparam name="T1"></typeparam>
        /// <typeparam name="T2"></typeparam>
        /// <typeparam name="T3"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="onSuccess">Action to perform on success of cloud workflow.</param>
        /// <param name="onFailure">Action to perform on failure of cloud workflow.</param>
        /// <returns></returns>
        public static LocalCloud<Unit> OnComplete<T1, T2, T3>(this LocalCloud<Tuple<T1, T2, T3>> workflow, Action<T1, T2, T3> onSuccess, Action<Exception> onFailure)
        {
            return CloudBuilder.OnComplete<Tuple<T1, T2, T3>>(workflow, (tuple => onSuccess.Invoke(tuple.Item1, tuple.Item2, tuple.Item3)), onFailure);
        }

        /// <summary>
        ///     Specifies set of actions to perform on completion of a cloud workflow.
        /// </summary>
        /// <typeparam name="T1"></typeparam>
        /// <typeparam name="T2"></typeparam>
        /// <typeparam name="T3"></typeparam>
        /// <typeparam name="T4"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="onSuccess">Action to perform on success of cloud workflow.</param>
        /// <param name="onFailure">Action to perform on failure of cloud workflow.</param>
        /// <returns></returns>
        public static LocalCloud<Unit> OnComplete<T1, T2, T3, T4>(this LocalCloud<Tuple<T1, T2, T3, T4>> workflow, Action<T1, T2, T3, T4> onSuccess, Action<Exception> onFailure)
        {
            return CloudBuilder.OnComplete<Tuple<T1, T2, T3, T4>>(workflow, (tuple => onSuccess.Invoke(tuple.Item1, tuple.Item2, tuple.Item3, tuple.Item4)), onFailure);
        }

        /// <summary>
        ///     Appens a finalizer action to cloud workflow.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="finalizer">Finalizer action.</param>
        /// <returns>A workflow that performs a finalizer action on completion.</returns>
        public static Cloud<T> Finally<T>(this Cloud<T> workflow, Action finalizer)
        {
            return Builders.cloud.TryFinally(workflow, finalizer.ToFSharpFunc());
        }

        /// <summary>
        ///     Appens a finalizer action to cloud workflow.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="finalizer">Finalizer action.</param>
        /// <returns>A workflow that performs a finalizer action on completion.</returns>
        public static LocalCloud<T> Finally<T>(this LocalCloud<T> workflow, Action finalizer)
        {
            return Builders.local.TryFinally(workflow, finalizer.ToFSharpFunc());
        }

        /// <summary>
        ///     wraps cloud workflow into a workflow that discards the returned result.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <returns>A cloud workflow that returns no result.</returns>
        public static Cloud<Unit> Ignore<T>(this Cloud<T> workflow)
        {
            return Cloud.Ignore(workflow);
        }

        /// <summary>
        ///     wraps cloud workflow into a workflow that discards the returned result.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <returns>A cloud workflow that returns no result.</returns>
        public static LocalCloud<Unit> Ignore<T>(this LocalCloud<T> workflow)
        {
            return Local.Ignore(workflow);
        }

        #endregion

        #region Local Parallel Combinators

        /// <summary>
        ///     Forks a local cloud workflow as a locally executing System.Threading.Task
        /// </summary>
        /// <typeparam name="T">Return type of the task.</typeparam>
        /// <param name="workflow">Workflow to be forked.</param>
        /// <param name="options">Task creation options.</param>
        /// <returns>A cloud process for tracking progress of forked execution.</returns>
        public static LocalCloud<Task<T>> StartAsTask<T>(this LocalCloud<T> workflow, TaskCreationOptions? options = null)
        {
            return MBrace.Core.Internals.Cloud.GetExecutionContext()
                .OnSuccess(ctx => MBrace.Core.Internals.Cloud.StartAsTask<T>(workflow, ctx.Resources.ToOption(), ctx.CancellationToken.ToOption(), Option.FromNullable(options)));
        }

        /// <summary>
        ///     Composes a collection of local workflows into a single
        ///     local parallel fork/join workflow. Computations are scheduled
        ///     using the local thread pool.
        /// </summary>
        /// <typeparam name="T">Child computation return type.</typeparam>
        /// <param name="children">Collection of child workflows to be executed.</param>
        /// <returns>A workflow that executes the children in parallel in the current process.</returns>
        public static LocalCloud<T[]> LocalParallel<T>(this IEnumerable<LocalCloud<T>> children)
        {
            return Core.Local.Parallel(children);
        }

        /// <summary>
        ///     Composes a collection of local workflows into a single
        ///     local parallel fork/join workflow. Computations are scheduled
        ///     using the local thread pool.
        /// </summary>
        /// <typeparam name="T">Child computation return type.</typeparam>
        /// <param name="children">Collection of child workflows to be executed.</param>
        /// <returns>A workflow that executes the children in parallel in the current process.</returns>
        public static LocalCloud<T[]> LocalParallel<T>(params LocalCloud<T>[] children)
        {
            return Core.Local.Parallel(children);
        }

        /// <summary>
        ///     Composes a collection of local non-deterministic workflows into a single
        ///     a parallel nondeterministic computation. Computations are schedules using the
        ///     local thread pool.
        /// </summary>
        /// <typeparam name="T">Child computation return type.</typeparam>
        /// <param name="children">Collection of local child workflows to be executed.</param>
        /// <returns>A workflow that executes the children in parallel in the current process.</returns>
        public static LocalCloud<FSharpOption<T>> LocalChoice<T>(this IEnumerable<LocalCloud<FSharpOption<T>>> children)
        {
            return Core.Local.Choice(children);
        }

        /// <summary>
        ///     Composes a collection of local non-deterministic workflows into a single
        ///     a parallel nondeterministic computation. Computations are schedules using the
        ///     local thread pool.
        /// </summary>
        /// <typeparam name="T">Child computation return type.</typeparam>
        /// <param name="children">Collection of local child workflows to be executed.</param>
        /// <returns>A workflow that executes the children in parallel in the current process.</returns>
        public static LocalCloud<FSharpOption<T>> LocalChoice<T>(params LocalCloud<FSharpOption<T>>[] children)
        {
            return Core.Local.Choice(children);
        }

        /// <summary>
        ///     Composes a collection of local non-deterministic workflows into a single
        ///     a parallel nondeterministic computation. Computations are schedules using the
        ///     local thread pool.
        /// </summary>
        /// <typeparam name="T">Child computation return type.</typeparam>
        /// <param name="children">Collection of local child workflows to be executed.</param>
        /// <returns>A workflow that executes the children in parallel in the current process.</returns>
        public static LocalCloud<T> LocalChoice<T>(this IEnumerable<LocalCloud<T>> children)
        {
            return children.Select(ch => ch.OnSuccess(t => t.ToOption())).LocalChoice().OnSuccess(t => t.Value);
        }

        /// <summary>
        ///     Composes a collection of local non-deterministic workflows into a single
        ///     a parallel nondeterministic computation. Computations are schedules using the
        ///     local thread pool.
        /// </summary>
        /// <typeparam name="T">Child computation return type.</typeparam>
        /// <param name="children">Collection of local child workflows to be executed.</param>
        /// <returns>A workflow that executes the children in parallel in the current process.</returns>
        public static LocalCloud<T> LocalChoice<T>(params LocalCloud<T>[] children)
        {
            return children.LocalChoice();
        }

        #endregion

        #region Distributed Parallel Combinators

        /// <summary>
        ///     Runs supplied cloud workflow as a forked cloud process,
        ///     returning a serializable handle to that process.
        /// </summary>
        /// <typeparam name="T">Return type of the cloud process.</typeparam>
        /// <param name="workflow">Workflow to be forked.</param>
        /// <param name="faultPolicy">Fault policy to be used by the process.</param>
        /// <param name="target">Target worker identifier to execute the process in.</param>
        /// <param name="cancellationToken">Cancellation token to be used for the process.</param>
        /// <param name="processName">Human-readable process description.</param>
        /// <returns>A cloud process for tracking progress of forked execution.</returns>
        public static Cloud<ICloudProcess<T>> CreateProcess<T>(this Cloud<T> workflow, FaultPolicy faultPolicy = null, IWorkerRef target = null,
                                                                    ICloudCancellationToken cancellationToken = null, string processName = null)
        {
            return Cloud.CreateProcess(workflow, Option.FromNullable(faultPolicy), Option.FromNullable(target), 
                                                Option.FromNullable(cancellationToken), Option.FromNullable(processName));
        }

        /// <summary>
        ///     Composes a collection of workflows into a single parallel
        ///     fork/join workflow.
        /// </summary>
        /// <typeparam name="T">Return type.</typeparam>
        /// <param name="children">Collection of child workflows.</param>
        /// <returns>A workflow that executes the children in parallel.</returns>
        public static Cloud<T[]> Parallel<T>(this IEnumerable<Cloud<T>> children)
        {
            var children2 = children.ToArray(); // interim solution for serialization errors
            return Core.Cloud.Parallel<Cloud<T>, T>(children2);
        }

        /// <summary>
        ///     Composes a collection of workflows into a single parallel
        ///     fork/join workflow.
        /// </summary>
        /// <typeparam name="T">Return type.</typeparam>
        /// <param name="children">Collection of child workflows.</param>
        /// <returns>A workflow that executes the children in parallel.</returns>
        public static Cloud<T[]> Parallel<T>(params Cloud<T>[] children)
        {
            return Core.Cloud.Parallel<Cloud<T>, T>(children);
        }

        /// <summary>
        ///     Composes a pair of workflows into one that executes both in parallel
        /// </summary>
        /// <typeparam name="T1">Left return type.</typeparam>
        /// <typeparam name="T2">Right return type.</typeparam>
        /// <param name="left">Left cloud workflow.</param>
        /// <param name="right">Right cloud workflow.</param>
        /// <returns></returns>
        public static Cloud<Tuple<T1,T2>> ParallelCombine<T1,T2>(this Cloud<T1> left, Cloud<T2> right)
        {
            return Core.Cloud.Parallel<T1, T2>(left, right);
        }

        /// <summary>
        ///     Performs a parallel iteration of supplied items across the cluster
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="items">Items to be iterated.</param>
        /// <param name="body">Body to be executed.</param>
        /// <returns>A cloud workflow that performs parallel for iteration.</returns>
        public static Cloud<Unit> ParallelForEach<T>(this IEnumerable<T> items, Func<T, LocalCloud<unit>> body)
        {
            var items2 = items.ToArray(); // interim solution for serialization errors
            return Library.Cloud.Balanced.iterLocal<T>(body.ToFSharpFunc(), items2);
        }

        /// <summary>
        ///     Performs a parallel mapper operation of supplied items across the cluster
        /// </summary>
        /// <typeparam name="T">Input element type.</typeparam>
        /// <typeparam name="R">Result element type.</typeparam>
        /// <param name="items">Input element sequence.</param>
        /// <param name="mapper">Mapping function.</param>
        /// <returns>A cloud workflow that performs parallel mapping operation.</returns>
        public static Cloud<R[]> ParallelMap<T, R>(this IEnumerable<T> items, Func<T, R> mapper)
        {
            var items2 = items.ToArray(); // interim solution for serialization errors
            return Library.Cloud.Balanced.map(mapper.ToFSharpFunc(), items2);
        }

        /// <summary>
        ///     Defines a parallel map/reduce workflow using supplied arguments.
        /// </summary>
        /// <param name="inputs">Input elements.</param>
        /// <typeparam name="T">Input element type.</typeparam>
        /// <typeparam name="R">Result type.</typeparam>
        /// <param name="mapper">Mapper function.</param>
        /// <param name="reducer">Reducer function.</param>
        /// <param name="init">Result initializer element.</param>
        /// <returns></returns>
        public static Cloud<R> MapReduce<T,R>(this IEnumerable<T> inputs, Func<T,R> mapper, Func<R,R,R> reducer, R init)
        {
            var inputs2 = inputs.ToArray(); // interim solution for serialization errors
            return Library.Cloud.Balanced.mapReduce(mapper.ToFSharpFunc(), reducer.ToFSharpFunc(), init, inputs2);
        }

        /// <summary>
        ///     Performs nondeterministic parallel computation with supplied children.
        ///     First computation to complete will cause the parent to complete.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="children">Children computations.</param>
        /// <returns>A workflow that executes the children in parallel nondeterminism.</returns>
        public static Cloud<FSharpOption<T>> Choice<T>(this IEnumerable<Cloud<FSharpOption<T>>> children)
        {
            var children2 = children.ToArray(); // interim solution for serialization errors
            return Core.Cloud.Choice<Cloud<FSharpOption<T>>, T>(children2);
        }

        /// <summary>
        ///     Performs nondeterministic parallel computation with supplied children.
        ///     First computation to complete will cause the parent to complete.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="children">Children computations.</param>
        /// <returns>A workflow that executes the children in parallel nondeterminism.</returns>
        public static Cloud<FSharpOption<T>> Choice<T>(params Cloud<FSharpOption<T>>[] children)
        {
            return Core.Cloud.Choice<Cloud<FSharpOption<T>>, T>(children);
        }

        /// <summary>
        ///     Performs nondeterministic parallel computation with supplied children.
        ///     First computation to complete will cause the parent to complete.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="children">Children computations.</param>
        /// <returns>A workflow that executes the children in parallel nondeterminism.</returns>
        public static Cloud<T> Choice<T>(this IEnumerable<Cloud<T>> children)
        {
            return children
                    .Select(c => c.OnSuccess(t => t.ToOption()))
                    .Choice()
                    .OnSuccess(t =>
                        {
                            T result;
                            if (!t.TryGetValue(out result))
                                throw new ArgumentException("Input is empty", "children");

                            return result;
                        });
        }

        #endregion
    }
}
