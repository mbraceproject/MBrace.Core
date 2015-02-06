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
    public abstract class SimpleTests
    {
        public abstract T Run<T>(Cloud<T> workflow);
        public void Run(CloudAction workflow)
        {
            this.Run(workflow.Body);
        }

        #region Parallel
        [Test]
        public void ParallelEmpty()
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
            var isSupported = this.Run(Cloud.IsTargetedWorkerSupported);
            if (isSupported)
            {
                var x = Cloud.Parallel(Cloud.FromValue(1));
                var y = Cloud.GetWorkerCount();

                Assert.AreEqual(this.Run(x).Sum(), this.Run(y));
            }
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
        #endregion

        #region Choice
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

        [Test]
        public void ChoiceAll()
        {
            var isSupported = this.Run(Cloud.IsTargetedWorkerSupported);
            if(isSupported)
            {
                var x = Cloud.Choice(Cloud.FromValue(1));
                Assert.AreEqual(1, this.Run(x));
            }
        }
        #endregion

        #region Other

        [Test]
        public void Sleep()
        {
           this.Run(Cloud.Sleep(100));
        }

        [Test]
        public void TryFinally()
        {
            var cref = this.Run(CloudRef.New(0, null, null));

            var workflow = Cloud.New(() => 
                    Cloud.TryFinally(
                        Cloud.FromValue(42), 
                        Cloud.Dispose(cref)));
            var result = this.Run(workflow);
            Assert.AreEqual(42, result);
            
            //Assert.Catch<Exception>(() => this.Run(cref.Value));
            // Workaround because above lambda captures 'this'.
            try
            {
                this.Run(cref.Value);
                Assert.Fail();
            }
            catch
            {
                ; // Success
            }
        }

        [Test]
        public void Ignore()
        {
            this.Run(Cloud.Ignore(Cloud.FromValue(42)));
        }

        [Test]
        public void GetTaskId()
        {
            var result = this.Run(Cloud.GetTaskId());
            Assert.True(!String.IsNullOrEmpty(result));
        }

        [Test]
        public void GetProcessId()
        {
            var result = this.Run(Cloud.GetProcessId());
            Assert.True(!String.IsNullOrEmpty(result));
        }

        [Test]
        public void GetAvailableWorkers()
        {
            var workflow =
                from workers in Cloud.GetAvailableWorkers()
                from w in Cloud.CurrentWorker
                select Tuple.Create(w, workers);

            var result = this.Run(workflow);

            Assert.Contains(result.Item1, result.Item2);
        }

        [Test]
        public void Dispose()
        {
            var cref = this.Run(CloudRef.New(0, null, null));
            this.Run(cref.Dispose());
            Assert.Catch<Exception>(() => this.Run(cref.Value));
        }

        [Test]
        public void AwaitTask()
        {
            var workflow = Cloud.New(() =>
                {
                    var task = Task.FromResult(42);
                    return Cloud.AwaitTask(task);
                });

            var result = this.Run(workflow);
            Assert.AreEqual(42, result);
        }
        #endregion
    }
}
