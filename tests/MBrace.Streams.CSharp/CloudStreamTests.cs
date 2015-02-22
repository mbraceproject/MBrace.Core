using FsCheck.Fluent;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MBrace.Streams.CSharp;
using Nessos.Streams.CSharp;
using System.IO;
using System.Runtime.CompilerServices;
using FsCheck;
using Microsoft.FSharp.Core;

namespace MBrace.Streams.CSharp.Tests
{
    [Serializable]
    internal class Custom1 { internal string Name; internal int Age; }

    [Serializable]
    internal class Custom2 { internal string Name { get; set; } internal int Age { get; set; } }

    internal static class CheckExtensions
    {
        internal static void QuickThrowOnFail<T>(this FSharpFunc<T, bool> test, int maxNb)
        {
            // Fluent.Configuration does not support ThrowOnFailure && Config type is F# record.
            var config = MBrace.Streams.Tests.Check.QuickThrowOnFailureConfig(maxNb);
            Check.One(config, test);
        }
    }

    [TestFixture]
    abstract public class CloudStreamsTests
    {
        abstract public T Run<T>(MBrace.Cloud<T> c);
        abstract public T RunLocal<T>(MBrace.Cloud<T> c);
        abstract public int MaxNumberOfTests { get; }

        internal void Run(MBrace.CSharp.CloudAction c)
        {
            this.Run(c.Body);
        }

        internal T[] Run<T>(IEnumerable<Cloud<T>> wfs)
        {
            return wfs.Select(wf => this.Run(wf)).ToArray();
        }

        [Test]
        public void OfArray()
        {
            FSharpFunc<int[], bool>.FromConverter(xs =>
                {
                    var x = xs.AsCloudStream().Select(i => i + 1).ToArray();
                    var y = xs.Select(i => i + 1).ToArray();
                    return this.Run(x).SequenceEqual(y);
                }).QuickThrowOnFail(this.MaxNumberOfTests);
        }

        [Test]
        public void OfCloudArray()
        {
            FSharpFunc<int[], bool>.FromConverter(xs =>
            {
                var ca = this.Run(CloudVector.New(xs, 100));
                var x = ca.AsCloudStream().Select(i => i + 1).ToArray();
                var y = xs.Select(i => i + 1).ToArray();
                return this.Run(x).SequenceEqual(y);
            }).QuickThrowOnFail(this.MaxNumberOfTests);
        }

        [Test]
        public void ToCloudArray()
        {
            FSharpFunc<int[], bool>.FromConverter(xs =>
            {
                var x = xs.AsCloudStream().Select(i => i + 1).ToCloudVector();
                var y = xs.Select(i => i + 1).ToArray();
                var r = this.RunLocal(this.Run(x).ToEnumerable());
                return r.SequenceEqual(y);
            }).QuickThrowOnFail(this.MaxNumberOfTests);
        }

        [Test]
        public void Cache()
        {
            FSharpFunc<int[], bool>.FromConverter(xs =>
            {
                var ca = this.Run(CloudVector.New(xs, 1024L));
                this.Run(CloudVector.Cache(ca));
                var x = ca.AsCloudStream().Select(i => i + 1).ToArray();
                var y = xs.Select(i => i + 1).ToArray();
                var z = ca.AsCloudStream().Select(i => i + 1).ToArray();
                return this.Run(x).SequenceEqual(y) &&
                       this.Run(z).SequenceEqual(y);
            }).QuickThrowOnFail(this.MaxNumberOfTests);
        }

        [Test]
        public void OfCloudFilesWithReadAllText()
        {
            FSharpFunc<string[], bool>.FromConverter(xs =>
            {
                var cfiles =
                    this.Run(xs.Select(text => MBrace.CloudFile.WriteAllText(text, null, null)));

                var x =
                    this.Run(cfiles
                        .AsCloudStream<string>(CloudFileReader.ReadAllText)
                        .ToArray());
                var y = cfiles.Select(f => this.RunLocal(CloudFile.ReadAllText(f, null)));

                var s1 = new HashSet<string>(x);
                var s2 = new HashSet<string>(y);
                return s1.SetEquals(s2);
            }).QuickThrowOnFail(10);
        }

        [Test]
        public void OfCloudFilesWithReadLines()
        {
            FSharpFunc<string[][], bool>.FromConverter(xs =>
            {
                var cfiles =
                    this.Run(xs.Select(text => MBrace.CloudFile.WriteAllLines(text, null, null)));

                var x =
                    this.Run(cfiles
                        .AsCloudStream(CloudFileReader.ReadLines)
                        .SelectMany(l => l.AsStream())
                        .ToArray());
                var y = cfiles.Select(f => this.RunLocal(CloudFile.ReadAllLines(f,null)))
                        .SelectMany(id => id);

                var s1 = new HashSet<string>(x);
                var s2 = new HashSet<string>(y);
                return s1.SetEquals(s2);
            }).QuickThrowOnFail(10);
        }

        [Test]
        public void OfCloudFilesWithReadAllLines()
        {
            FSharpFunc<string[][], bool>.FromConverter(xs =>
            {
                var cfiles =
                    this.Run(xs.Select(text => MBrace.CloudFile.WriteAllLines(text, null, null)));

                var x =
                    this.Run(cfiles
                        .AsCloudStream(CloudFileReader.ReadAllLines)
                        .SelectMany(l => l.AsStream())
                        .ToArray());

                var y = cfiles.Select(f => this.RunLocal(CloudFile.ReadAllLines(f, null)))
                        .SelectMany(id => id);

                var s1 = new HashSet<string>(x);
                var s2 = new HashSet<string>(y);
                return s1.SetEquals(s2);
            }).QuickThrowOnFail(10);
        }

        [Test]
        public void Select()
        {
            FSharpFunc<int[], bool>.FromConverter(xs =>
            {
                var x = xs.AsCloudStream().Select(i => i + 1).ToArray();
                var y = xs.AsParallel().Select(i => i + 1).ToArray();
                return this.Run(x).SequenceEqual(y);
            }).QuickThrowOnFail(this.MaxNumberOfTests);
        }

        [Test]
        public void Where()
        {
            FSharpFunc<int[], bool>.FromConverter(xs =>
            {
                var x = xs.AsCloudStream().Where(i => i % 2 == 0).ToArray();
                var y = xs.AsParallel().Where(i => i % 2 == 0).ToArray();
                return this.Run(x).SequenceEqual(y);
            }).QuickThrowOnFail(this.MaxNumberOfTests);
        }

        [Test]
        public void SelectMany()
        {
            FSharpFunc<int[], bool>.FromConverter(xs =>
            {
                var x = xs.AsCloudStream().SelectMany<int, int>(i => xs.AsStream()).ToArray();
                var y = xs.AsParallel().SelectMany(i => xs).ToArray();
                return this.Run(x).SequenceEqual(y);
            }).QuickThrowOnFail(this.MaxNumberOfTests);
        }


        [Test]
        public void Aggregate()
        {
            FSharpFunc<int[], bool>.FromConverter(xs =>
            {
                var x = xs.AsCloudStream().Select(i => i + 1).Aggregate(() => 0, (acc, i) => acc + i, (left, right) => left + right);
                var y = xs.AsParallel().Select(i => i + 1).Aggregate(() => 0, (acc, i) => acc + i, (left, right) => left + right, i => i);
                return this.Run(x) == y;
            }).QuickThrowOnFail(this.MaxNumberOfTests);
        }


        [Test]
        public void Sum()
        {
            FSharpFunc<int[], bool>.FromConverter(xs =>
            {
                var x = xs.AsCloudStream().Select(i => i + 1).Sum();
                var y = xs.AsParallel().Select(i => i + 1).Sum();
                return this.Run(x) == y;
            }).QuickThrowOnFail(this.MaxNumberOfTests);
        }

        [Test]
        public void Count()
        {
            FSharpFunc<int[], bool>.FromConverter(xs =>
            {
                var x = xs.AsCloudStream().Select(i => i + 1).Count();
                var y = xs.AsParallel().Select(i => i + 1).Count();
                return this.Run(x) == y;
            }).QuickThrowOnFail(this.MaxNumberOfTests);
        }

        [Test]
        public void OrderBy()
        {
            FSharpFunc<int[], bool>.FromConverter(xs =>
            {
                var x = xs.AsCloudStream().Select(i => i + 1).OrderBy(i => i, 10).ToArray();
                var y = xs.AsParallel().Select(i => i + 1).OrderBy(i => i).Take(10).ToArray();
                return this.Run(x).SequenceEqual(y);
            }).QuickThrowOnFail(this.MaxNumberOfTests);
        }

        [Test]
        public void CustomObject1()
        {
            FSharpFunc<int[], bool>.FromConverter(xs =>
            {
                var x = xs.AsCloudStream().Select(i => new Custom1 { Name = i.ToString(), Age = i }).ToArray();
                var y = xs.AsParallel().Select(i => new Custom1 { Name = i.ToString(), Age = i }).ToArray();
                return this.Run(x).Zip(y, (l, r) => l.Name == r.Name && l.Age == r.Age).All(b => b == true);
            }).QuickThrowOnFail(this.MaxNumberOfTests);
        }

        [Test]
        public void CustomObject2()
        {
            FSharpFunc<int[], bool>.FromConverter(xs =>
            {
                var x = xs.AsCloudStream().Select(i => new Custom2 { Name = i.ToString(), Age = i }).ToArray();
                var y = xs.AsParallel().Select(i => new Custom2 { Name = i.ToString(), Age = i }).ToArray();
                return this.Run(x).Zip(y, (l, r) => l.Name == r.Name && l.Age == r.Age).All(b => b == true);
            }).QuickThrowOnFail(this.MaxNumberOfTests);
        }

        [Test]
        public void AnonymousType()
        {
            FSharpFunc<int[], bool>.FromConverter(xs =>
            {
                var x = xs.AsCloudStream().Select(i => new { Value = i }).ToArray();
                var y = xs.AsParallel().Select(i => new { Value = i }).ToArray();
                return this.Run(x).SequenceEqual(y);
            }).QuickThrowOnFail(this.MaxNumberOfTests);
        }

        [Test]
        public void CapturedVariable()
        {
            FSharpFunc<int[], bool>.FromConverter(xs =>
            {
                var ys = Enumerable.Range(1, 10).ToArray();
                var x = xs.AsCloudStream().SelectMany<int, int>(_ => ys.AsStream()).ToArray();
                var y = xs.AsParallel().SelectMany<int, int>(_ => ys).ToArray();
                return this.Run(x).SequenceEqual(y);
            }).QuickThrowOnFail(this.MaxNumberOfTests);
        }

        [Test]
        public void ComprehensionSyntax()
        {
            FSharpFunc<int[], bool>.FromConverter(xs =>
            {
                var x = (from x1 in xs.AsCloudStream()
                         select x1 * x1).ToArray();
                var y = (from x2 in xs.AsParallel()
                         select x2 * x2).ToArray();
                return this.Run(x).SequenceEqual(y);
            }).QuickThrowOnFail(this.MaxNumberOfTests);
        }
    }
}
