using MBrace.Core;
using MBrace.Flow;
using MBrace.Core.Tests;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MBrace.Flow.CSharp;
using Nessos.Streams.CSharp;
using System.IO;
using System.Runtime.CompilerServices;
using FsCheck;
using Microsoft.FSharp.Core;

namespace MBrace.Flow.CSharp.Tests
{

    internal static class CheckExtensions
    {
        internal static void QuickThrowOnFail<T>(this FSharpFunc<T, bool> test, int maxNb)
        {
            Utils.Check.QuickThrowOnFail(test, FSharpOption<int>.Some(maxNb));
        }
    }

    [TestFixture]
    abstract public class CloudFlowTests
    {
        abstract public T Run<T>(Cloud<T> c);
        abstract public T RunLocally<T>(Cloud<T> c);
        abstract public string[] RunWithLogs(Cloud<Unit> c);
        abstract public int FsCheckMaxNumberOfTests { get; }
        abstract public int FsCheckMaxNumberOfIOBoundTests { get; }

        [Test]
        public void OfArray_ToArray()
        {
            FSharpFunc<int[], bool>.FromConverter(xs =>
                {
                    var x = CloudFlow.OfArray(xs).ToArray();
                    var y = xs.ToArray();
                    return this.Run(x).SequenceEqual(y);
                }).QuickThrowOnFail(this.FsCheckMaxNumberOfTests);
        }

        [Test]
        public void Select()
        {
            FSharpFunc<int[], bool>.FromConverter(xs =>
            {
                var x = CloudFlow.OfArray(xs).Select(v => v * 2).ToArray();
                var y = xs.Select(v => v * 2).ToArray();
                return this.Run(x).SequenceEqual(y);
            }).QuickThrowOnFail(this.FsCheckMaxNumberOfTests);
        }

        [Test]
        public void Where()
        {
            FSharpFunc<int[], bool>.FromConverter(xs =>
            {
                var x = CloudFlow.OfArray(xs).Where(v => v % 2 == 0).ToArray();
                var y = xs.Where(v => v % 2 == 0).ToArray();
                return this.Run(x).SequenceEqual(y);
            }).QuickThrowOnFail(this.FsCheckMaxNumberOfTests);
        }


        [Test]
        public void SelectMany()
        {
            FSharpFunc<int[], bool>.FromConverter(xs =>
            {
                var x = CloudFlow.OfArray(xs).SelectMany(v => Enumerable.Range(1, 1000)).ToArray();
                var y = xs.SelectMany(v => Enumerable.Range(1, 1000)).ToArray();
                return this.Run(x).SequenceEqual(y);
            }).QuickThrowOnFail(this.FsCheckMaxNumberOfTests);
        }

        [Test]
        public void Count()
        {
            FSharpFunc<int[], bool>.FromConverter(xs =>
            {
                var x = CloudFlow.OfArray(xs).Where(v => v % 2 == 0).Count();
                var y = xs.Where(v => v % 2 == 0).Count();
                return this.Run(x) == y;
            }).QuickThrowOnFail(this.FsCheckMaxNumberOfTests);
        }

        [Test]
        public void Sum()
        {
            FSharpFunc<int[], bool>.FromConverter(xs =>
            {
                var x = CloudFlow.OfArray(xs).Select(v => v * 2).Sum();
                var y = xs.Select(v => v * 2).Sum();
                return this.Run(x) == y;
            }).QuickThrowOnFail(this.FsCheckMaxNumberOfTests);
        }

        [Test]
        public void Take()
        {
            FSharpFunc<Tuple<int[], int>, bool>.FromConverter(t =>
            {
                var xs = t.Item1;
                var n = Math.Abs(t.Item2);
                var x = CloudFlow.OfArray(xs).Take(n).ToArray();
                var y = xs.Take(n).ToArray();
                return this.Run(x).SequenceEqual(y);
            }).QuickThrowOnFail(this.FsCheckMaxNumberOfTests);
        }


        [Test]
        public void OrderBy()
        {
            FSharpFunc<Tuple<int[], int>, bool>.FromConverter(t =>
            {
                var xs = t.Item1;
                var n = Math.Abs(t.Item2);
                var x = CloudFlow.OfArray(xs).OrderBy(v => v, n).ToArray();
                var y = xs.OrderBy(v => v).Take(n).ToArray();
                return this.Run(x).SequenceEqual(y);
            }).QuickThrowOnFail(this.FsCheckMaxNumberOfTests);
        }

        [Test]
        public void OrderByDescending()
        {
            FSharpFunc<Tuple<int[], int>, bool>.FromConverter(t =>
            {
                var xs = t.Item1;
                var n = Math.Abs(t.Item2);
                var x = CloudFlow.OfArray(xs).OrderByDescending(v => v, n).ToArray();
                var y = xs.OrderByDescending(v => v).Take(n).ToArray();
                return this.Run(x).SequenceEqual(y);
            }).QuickThrowOnFail(this.FsCheckMaxNumberOfTests);
        }

        [Test]
        public void CountBy()
        {
            FSharpFunc<int[], bool>.FromConverter(xs =>
            {
                var x = CloudFlow.OfArray(xs).CountBy(v => v).ToArray();
                var y = xs.GroupBy(v => v).Select(v => Tuple.Create(v.Key, (long)v.Count())).ToArray();
                return new HashSet<Tuple<int, long>>(this.Run(x)).SetEquals(new HashSet<Tuple<int, long>>(y));
            }).QuickThrowOnFail(this.FsCheckMaxNumberOfTests);
        }

        [Test]
        public void GroupBy()
        {
            FSharpFunc<int[], bool>.FromConverter(xs =>
            {
                var x = CloudFlow.OfArray(xs).GroupBy(v => v).Select(t => Tuple.Create(t.Item1, t.Item2.Count())).ToArray();
                var y = xs.GroupBy(v => v).Select(v => Tuple.Create(v.Key, v.Count())).ToArray();
                return new HashSet<Tuple<int, int>>(this.Run(x)).SetEquals(new HashSet<Tuple<int, int>>(y));
            }).QuickThrowOnFail(this.FsCheckMaxNumberOfTests);
        }

        [Test]
        public void Aggregate()
        {
            FSharpFunc<int[], bool>.FromConverter(xs =>
            {
                var x = CloudFlow.OfArray(xs).Select(v => v * 2).Aggregate(() => 0, (acc, v) => acc + v, (l, r) => l + r);
                var y = xs.Select(v => v * 2).Aggregate(0, (acc, v) => acc + v);
                return this.Run(x) == y;
            }).QuickThrowOnFail(this.FsCheckMaxNumberOfTests);
        }

        [Test]
        public void AggregateBy()
        {
            FSharpFunc<int[], bool>.FromConverter(xs =>
            {
                var x = CloudFlow.OfArray(xs).AggregateBy(v => v, () => 0, (acc, v) => acc + v, (l, r) => l + r).ToArray();
                var y = xs.GroupBy(v => v).Select(v => Tuple.Create(v.Key, v.Sum())).ToArray();
                return new HashSet<Tuple<int, int>>(this.Run(x)).SetEquals(new HashSet<Tuple<int, int>>(y));
            }).QuickThrowOnFail(this.FsCheckMaxNumberOfTests);
        }

        [Test]
        public void Cache()
        {
            FSharpFunc<int[], bool>.FromConverter(xs =>
            {
                var flow = this.Run(CloudFlow.OfArray(xs).Cache());
                var x = this.Run(flow.Select(v => v * 2).ToArray());
                var count = this.Run(Cloud.GetWorkerCount());
                var y = xs.Select(v => v * 2).ToArray();
                return x.SequenceEqual(y);
            }).QuickThrowOnFail(this.FsCheckMaxNumberOfTests);
        }

    }
}
