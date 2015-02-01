using MBrace.SampleRuntime;
using Microsoft.FSharp.Core;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace MBrace.CSharp.Tests
{
    [TestFixture]
    public class SampleRuntimeTests : SimpleTests
    {

        MBraceRuntime rt;

        string GetFileDir([CallerFilePath]string file = "") { return file; }

        [TestFixtureSetUp]
        public void SetUp()
        {
            var path = Path.GetDirectoryName(this.GetFileDir());
            MBraceRuntime.WorkerExecutable = Path.Combine(path, "../../bin/MBrace.SampleRuntime.exe");
            rt = MBraceRuntime.InitLocal(3);
        }

        [TestFixtureTearDown]
        public void TearDown()
        {
            rt.KillAllWorkers();
        }

        public override T Run<T>(Cloud<T> c)
        {
            return rt.Run(c.Computation, null, FSharpOption<FaultPolicy>.Some(FaultPolicy.NoRetry));
        }

    }

    [TestFixture]
    public abstract class SimpleTests
    {
        public abstract T Run<T>(Cloud<T> workflow);

        [Test]
        public void ParallelEmpty ()
        {
            var x = Enumerable.Empty<Cloud<int>>()
                    .Parallel();
            Assert.IsEmpty(this.Run(x));
        }

        [Test]
        public void ParallelSimple()
        {
            var input = Enumerable.Range(1, 10);
            var x = input
                        .Select(i => Cloud.FromValue(i))
                        .Parallel();
            Assert.True(this.Run(x).SequenceEqual(input));
        }

        [Test]
        public void ParallelAll()
        {
            var x = Cloud.Parallel(Cloud.FromValue(1));
            var y = Cloud.GetWorkerCount();

            Assert.AreEqual(this.Run(x).Sum(), this.Run(y));
        }

        [Test]
        public void ParallelException()
        {
            var x = 
                Cloud.Parallel(
                      Cloud.FromValue(42),
                      Cloud.Throw<int>(new DivideByZeroException()));

            Assert.Throws<DivideByZeroException>(() => this.Run(x));
        }


        [Test]
        public void ChoiceEmpty1()
        {
            //Delay argument check.
            var x = Cloud.New(() => Enumerable.Empty<Cloud<int>>().Choice());
            Assert.Throws<ArgumentException>(() => this.Run(x));
        }

        [Test]
        public void ChoiceEmpty2()
        {
            var x = Enumerable.Empty<Cloud<Option<int>>>()
                    .Choice();
            Assert.False(this.Run(x).HasValue);
        }

        [Test]
        public void ChoiceSimple1()
        {
            var input = Enumerable.Range(1, 10);
            var x = input
                        .Select(i =>
                            Cloud.Sleep(1000 * i)
                            .Then(() => Cloud.FromValue(i)))
                        .Choice();
            Assert.AreEqual(1, this.Run(x));
        }

        [Test]
        public void ChoiceSimple2()
        {
            var input = Enumerable.Range(1, 10);
            var x = input
                        .Select(i => 
                            Cloud.Sleep(1000 * i)
                            .Then(() => Cloud.FromValue(Option<int>.Some(i))))
                        .Choice();
            Assert.AreEqual(1, this.Run(x).Value);
        }

    }
}
