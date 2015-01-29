using FsCheck.Fluent;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MBrace.Streams.CSharp;
using Nessos.Streams.CSharp;
using MBrace.SampleRuntime;
using System.IO;
using System.Runtime.CompilerServices;

namespace MBrace.Streams.CSharp.Tests
{
    [Serializable]
    internal class Custom1 { internal string Name; internal int Age; }

    [Serializable]
    internal class Custom2 { internal string Name { get; set; } internal int Age { get; set; } }

    public class SampleRuntimeCloudStreamTests : CloudStreamsTests
    {
        Configuration c = new Configuration() { MaxNbOfTest = 10 };
        MBraceRuntime rt;

        string GetFileDir([CallerFilePath]string file = "") { return file; }

        [TestFixtureSetUp]
        public void SetUp()
        { 
            var path = Path.GetDirectoryName(this.GetFileDir());
            MBraceRuntime.WorkerExecutable = Path.Combine(path, "../../lib/MBrace.SampleRuntime.exe");
            rt = MBraceRuntime.InitLocal(3);
        }

        [TestFixtureTearDown]
        public void TearDown()
        {
            rt.KillAllWorkers();
        }

        internal override T Evaluate<T>(Cloud<T> c)
        {
            return rt.Run(c, null, null);
        }

        internal override Configuration config
        {
            get { return c; }
        }
    }


    [TestFixture]
    abstract public class CloudStreamsTests
    {
        abstract internal T Evaluate<T>(MBrace.Cloud<T> c);
        abstract internal Configuration config { get; }

        internal T[] Evaluate<T>(IEnumerable<Cloud<T>> wfs)
        {
            return wfs.Select(wf => this.Evaluate(wf)).ToArray();
        }

        [Test]
        public void OfArray()
        {
            Spec.ForAny<int[]>(xs =>
            {
                var x = xs.AsCloudStream().Select(i => i + 1).ToArray();
                var y = xs.Select(i => i + 1).ToArray();
                return this.Evaluate(x).SequenceEqual(y);
            }).Check(config);
        }

        [Test]
        public void OfCloudArray()
        {
            Spec.ForAny<int[]>(xs =>
            {
                var ca = this.Evaluate(CloudArray.New(xs, null, null, null));
                var x = ca.AsCloudStream().Select(i => i + 1).ToArray();
                var y = ca.Select(i => i + 1).ToArray();
                return this.Evaluate(x).SequenceEqual(y);
            }).Check(config);
        }

        [Test]
        public void ToCloudArray()
        {
            Spec.ForAny<int[]>(xs =>
            {
                var x = xs.AsCloudStream().Select(i => i + 1).ToCloudArray();
                var y = xs.Select(i => i + 1).ToArray();
                return this.Evaluate(x).SequenceEqual(y);
            }).Check(config);
        }

        [Test]
        public void Cache()
        {
            Spec.ForAny<int[]>(xs =>
            {
                var ca = this.Evaluate(CloudArray.New(xs, null, null, null));
                var cached = this.Evaluate(ca.Cache());
                var x = cached.AsCloudStream().Select(i => i + 1).ToArray();
                var y = xs.Select(i => i + 1).ToArray();
                var z = ca.AsCloudStream().Select(i => i + 1).ToArray();
                return this.Evaluate(x).SequenceEqual(y) &&
                       this.Evaluate(z).SequenceEqual(y);
            }).Check(config);
        }

        [Test]
        public void SubsequentCache()
        {
            Spec.ForAny<int[]>(xs =>
            {
                var ca = this.Evaluate(CloudArray.New(xs, null, null, null));
                var _ = this.Evaluate(ca.Cache());
                var cached = this.Evaluate(ca.Cache());
                var x = cached.AsCloudStream().Select(i => i + 1).ToArray();
                var y = xs.Select(i => i + 1).ToArray();
                var z = ca.AsCloudStream().Select(i => i + 1).ToArray();
                return this.Evaluate(x).SequenceEqual(y) &&
                       this.Evaluate(z).SequenceEqual(y);
            }).Check(config);
        }

        [Test]
        public void OfCloudFiles_ReadAllText()
        {
            Spec.ForAny<string[]>(xs =>
            {
                var cfiles =
                    this.Evaluate(xs.Select(text => MBrace.CloudFile.WriteAllText(text, null, null)));

                var x =
                    this.Evaluate(cfiles
                        .AsCloudStream<string>(CloudFileReader.ReadAllText)
                        .ToArray());
                var y = this.Evaluate(cfiles.Select(cf => MBrace.CloudFile.ReadAllText(cf, null)));

                var s1 = new HashSet<string>(x);
                var s2 = new HashSet<string>(y);
                return s1.SetEquals(s2);
            }).Check(config);
        }

        [Test]
        public void OfCloudFiles_ReadLines()
        {
            Spec.ForAny<string[]>(xs =>
            {
                var cfiles =
                    this.Evaluate(xs.Select(text => MBrace.CloudFile.WriteLines(text.Split('\n'), null, null)));

                var x =
                    this.Evaluate(cfiles
                        .AsCloudStream(CloudFileReader.ReadLines)
                        .SelectMany(l => l.AsStream())
                        .ToArray());
                var y = this.Evaluate(cfiles
                        .Select(cf => MBrace.CloudFile.ReadLines(cf, null))
                        ).SelectMany(l => l);

                var s1 = new HashSet<string>(x);
                var s2 = new HashSet<string>(y);
                return s1.SetEquals(s2);
            }).Check(config);
        }

        [Test]
        public void OfCloudFiles_ReadAllLines()
        {
            Spec.ForAny<string[]>(xs =>
            {
                var cfiles =
                    this.Evaluate(xs.Select(text => MBrace.CloudFile.WriteLines(text.Split('\n'), null, null)));

                var x =
                    this.Evaluate(cfiles
                        .AsCloudStream(CloudFileReader.ReadAllLines)
                        .SelectMany(l => l.AsStream())
                        .ToArray());
                var y = this.Evaluate(cfiles
                        .Select(cf => MBrace.CloudFile.ReadLines(cf, null))
                        ).SelectMany(l => l);

                var s1 = new HashSet<string>(x);
                var s2 = new HashSet<string>(y);
                return s1.SetEquals(s2);
            }).Check(config);
        }

        [Test]
        public void Select()
        {
            Spec.ForAny<int[]>(xs =>
            {
                var x = xs.AsCloudStream().Select(i => i + 1).ToArray();
                var y = xs.AsParallel().Select(i => i + 1).ToArray();
                return this.Evaluate(x).SequenceEqual(y);
            }).Check(config);
        }

        [Test]
        public void Where()
        {
            Spec.ForAny<int[]>(xs =>
            {
                var x = xs.AsCloudStream().Where(i => i % 2 == 0).ToArray();
                var y = xs.AsParallel().Where(i => i % 2 == 0).ToArray();
                return this.Evaluate(x).SequenceEqual(y);
            }).Check(config);
        }

        [Test]
        public void SelectMany()
        {
            Spec.ForAny<int[]>(xs =>
            {
                var x = xs.AsCloudStream().SelectMany<int, int>(i => xs.AsStream()).ToArray();
                var y = xs.AsParallel().SelectMany(i => xs).ToArray();
                return this.Evaluate(x).SequenceEqual(y);
            }).Check(config);
        }


        [Test]
        public void Aggregate()
        {
            Spec.ForAny<int[]>(xs =>
            {
                var x = xs.AsCloudStream().Select(i => i + 1).Aggregate(() => 0, (acc, i) => acc + i, (left, right) => left + right);
                var y = xs.AsParallel().Select(i => i + 1).Aggregate(() => 0, (acc, i) => acc + i, (left, right) => left + right, i => i);
                return this.Evaluate(x) == y;
            }).Check(config);
        }


        [Test]
        public void Sum()
        {
            Spec.ForAny<int[]>(xs =>
            {
                var x = xs.AsCloudStream().Select(i => i + 1).Sum();
                var y = xs.AsParallel().Select(i => i + 1).Sum();
                return this.Evaluate(x) == y;
            }).Check(config);
        }

        [Test]
        public void Count()
        {
            Spec.ForAny<int[]>(xs =>
            {
                var x = xs.AsCloudStream().Select(i => i + 1).Count();
                var y = xs.AsParallel().Select(i => i + 1).Count();
                return this.Evaluate(x) == y;
            }).Check(config);
        }

        [Test]
        public void OrderBy()
        {
            Spec.ForAny<int[]>(xs =>
            {
                var x = xs.AsCloudStream().Select(i => i + 1).OrderBy(i => i, 10).ToArray();
                var y = xs.AsParallel().Select(i => i + 1).OrderBy(i => i).Take(10).ToArray();
                return this.Evaluate(x).SequenceEqual(y);
            }).Check(config);
        }

        [Test]
        public void CustomObject1()
        {
            Spec.ForAny<int[]>(xs =>
            {
                var x = xs.AsCloudStream().Select(i => new Custom1 { Name = i.ToString(), Age = i }).ToArray();
                var y = xs.AsParallel().Select(i => new Custom1 { Name = i.ToString(), Age = i }).ToArray();
                return this.Evaluate(x).Zip(y, (l, r) => l.Name == r.Name && l.Age == r.Age).All(b => b == true);
            }).Check(config);
        }

        [Test]
        public void CustomObject2()
        {
            Spec.ForAny<int[]>(xs =>
            {
                var x = xs.AsCloudStream().Select(i => new Custom2 { Name = i.ToString(), Age = i }).ToArray();
                var y = xs.AsParallel().Select(i => new Custom2 { Name = i.ToString(), Age = i }).ToArray();
                return this.Evaluate(x).Zip(y, (l, r) => l.Name == r.Name && l.Age == r.Age).All(b => b == true);
            }).Check(config);
        }

        [Test]
        public void AnonymousType()
        {
            Spec.ForAny<int[]>(xs =>
            {
                var x = xs.AsCloudStream().Select(i => new { Value = i }).ToArray();
                var y = xs.AsParallel().Select(i => new { Value = i }).ToArray();
                return this.Evaluate(x).SequenceEqual(y);
            }).Check(config);
        }

        [Test]
        public void CapturedVariable()
        {
            Spec.ForAny<int[]>(xs =>
            {
                var ys = Enumerable.Range(1, 10).ToArray();
                var x = xs.AsCloudStream().SelectMany<int, int>(_ => ys.AsStream()).ToArray();
                var y = xs.AsParallel().SelectMany<int, int>(_ => ys).ToArray();
                return this.Evaluate(x).SequenceEqual(y);
            }).Check(config);
        }

        [Test]
        public void ComprehensionSyntax()
        {
            Spec.ForAny<int[]>(xs =>
            {
                var x = (from x1 in xs.AsCloudStream()
                         select x1 * x1).ToArray();
                var y = (from x2 in xs.AsParallel()
                         select x2 * x2).ToArray();
                return this.Evaluate(x).SequenceEqual(y);
            }).Check(config);
        }
    }
}
