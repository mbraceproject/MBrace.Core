﻿using System;
using System.Collections.Generic;
using System.Linq;
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
        ///     Binds result of provided workflow to supplied continuation lambda.
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
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="S"></typeparam>
        /// <param name="workflowA">This workflow.</param>
        /// <param name="workflowB">Result continuation.</param>
        /// <returns>A cloud workflow composed of the two.</returns>
        public static Cloud<Tuple<T, S>> Combine<T, S>(this Cloud<T> workflowA, Cloud<S> workflowB)
        {
            return workflowA.Bind(t => workflowB.OnSuccess(s => new Tuple<T, S>(t, s)));
        }

        /// <summary>
        ///     Binds result of provided workflow to supplied continuation lambda.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="S"></typeparam>
        /// <param name="workflowA">This workflow.</param>
        /// <param name="workflowB">Result continuation.</param>
        /// <returns>A cloud workflow composed of the two.</returns>
        public static LocalCloud<Tuple<T,S>> Combine<T,S>(this LocalCloud<T> workflowA, LocalCloud<S> workflowB)
        {
            return workflowA.Bind(t => workflowB.OnSuccess(s => new Tuple<T, S>(t, s)));
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

        /// <summary>
        ///     Combines cloud workflow with a subsequent computation.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="S"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="then">Computation to combine with.</param>
        /// <returns>Combined cloud workflow.</returns>
        public static Cloud<S> Bind<T,S>(this Cloud<T> workflow, Cloud<S> then)
        {
            return Builders.cloud.Combine(Core.Cloud.Ignore(workflow), then);
        }

        /// <summary>
        ///     Combines cloud workflow with a subsequent computation.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="S"></typeparam>
        /// <param name="workflow">This workflow.</param>
        /// <param name="then">Computation to combine with.</param>
        public static LocalCloud<S> Bind<T, S>(this LocalCloud<T> workflow, LocalCloud<S> then)
        {
            return Builders.local.Combine(Local.Ignore(workflow), then);
        }

        #endregion

        #region Parallel Combinators

        /// <summary>
        ///     Runs supplied cloud workflow as a forked cloud process,
        ///     returning a serializable handle to that process.
        /// </summary>
        /// <typeparam name="T">Return type of the cloud process.</typeparam>
        /// <param name="workflow">Workflow to be forked.</param>
        /// <returns>A cloud process for tracking progress of forked execution.</returns>
        public static Cloud<ICloudProcess<T>> CreateProcess<T>(this Cloud<T> workflow)
        {
            return Cloud.CreateProcess(workflow);
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
        /// <typeparam name="T">Left return type.</typeparam>
        /// <typeparam name="S">Right return type.</typeparam>
        /// <param name="left">Left cloud workflow.</param>
        /// <param name="right">Right cloud workflow.</param>
        /// <returns></returns>
        public static Cloud<Tuple<T,S>> ParallelCombine<T,S>(this Cloud<T> left, Cloud<S> right)
        {
            return Core.Cloud.Parallel<T, S>(left, right);
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