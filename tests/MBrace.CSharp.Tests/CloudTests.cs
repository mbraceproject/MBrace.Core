using System;
using System.Collections.Generic;
using System.Linq;
using NUnit.Framework;
using Microsoft.FSharp.Core;
using MBrace.Core;
using MBrace.Core.CSharp;

namespace MBrace.CSharp.Tests
{
    [TestFixture]
    abstract public class CloudTests
    {
        abstract public T Run<T>(Cloud<T> c);
        abstract public T RunLocally<T>(Cloud<T> c);
        abstract public string[] RunWithLogs(Cloud<Unit> c);

        [Test]
        public void Simple_Cloud_Workflow()
        {
            var workflow =
                CloudBuilder
                    .FromValue(21)
                    .OnSuccess(x => x * 2)
                    .OnSuccess(x => Assert.AreEqual(42, x));

            this.Run(workflow);
        }

        [Test]
        public void Simple_Parallel_Workflow()
        {
            var expected = Enumerable.Range(1, 100).Select(x => x * x).Sum();
            var sqr = CloudBuilder.FromFunc((int x) => x * x);
            var children = Enumerable.Range(1, 100).Select(sqr).ToArray();
            var workflow =
                CloudBuilder
                    .Parallel(children)
                    .OnSuccess(results => results.Sum())
                    .OnSuccess(sum => Assert.AreEqual(expected, sum));

            this.Run(workflow);
        }

        [Test]
        public void Simple_Parallel_ForEach()
        {
            var N = 1000;
            var workflow =
                CloudBuilder
                    .ParallelForEach(Enumerable.Range(1, N), x => CloudBuilder.Log("I'm log entry #{0}", x))
                    .Then(CloudBuilder.Sleep(5000));

            var logs = this.RunWithLogs(workflow);
            Assert.AreEqual(N, logs.Length);
        }

        [Test]
        public void Simple_Parallel_Map()
        {
            var inputs = Enumerable.Range(1, 1000);
            var expected = inputs.Select(x => x * x).Sum();
            var workflow =
                CloudBuilder
                    .ParallelMap(inputs, i => i * i)
                    .OnSuccess(xs => xs.Sum())
                    .OnSuccess(sum => Assert.AreEqual(expected, sum));

            this.Run(workflow);
        }

        [Test]
        public void Simple_Map_Reduce()
        {
            string[] texts =
                new string[] {
                    "Lorem ipsum dolor sit amet, mea ferri alienum efficiantur cu, periculis principes complectitur ius te. Eum te quaerendum delicatissimi, at consul audiam eripuit mel, eu vide cibo facilisis mel. Saepe blandit vix et, dicat facilisis comprehensam ea vis. No vis hinc vivendo fabellas, atqui dicunt vel ad.",
                    "Cu quodsi percipit has, unum tincidunt dissentiunt eum ne, at oratio latine vel. Debet consul cum id, nec illum debet eruditi no. His audire reformidans id, reque theophrastus in mea. Usu saepe nostro sensibus te, mel inimicus gubergren no.",
                    "Duo id inermis noluisse pericula, no sit solet deserunt definitionem. Summo oporteat te vix, sanctus iudicabit honestatis ad eum. Et tritani corpora albucius eos, et mei utroque graecis fabellas. Ex enim aperiri sea, no quo prodesset referrentur, vel utamur diceret eu. Sed laudem noluisse luptatum in. Legere inermis ullamcorper duo ut.",
                    "Id vim vocent urbanitas theophrastus, vim in elit homero civibus. Ut augue mentitum adipisci eam, eum id velit possim. No sea nullam vocibus. Inani aliquam quo ex, sonet vitae detraxit in nec, ut legere indoctum consectetuer his. Ei tale nominati disputationi mei." };

            Func<string, Tuple<string, int>[]> mapper =
                text =>
                    text.Split(new char[] { ',', ' ', '.' })
                        .Select(w => w.ToLower().Trim())
                        .GroupBy(w => w)
                        .Select(gp => new Tuple<string, int>(gp.Key, gp.Count()))
                        .ToArray();

            Func<Tuple<string, int>[], Tuple<string, int>[], Tuple<string, int>[]> reducer =
                (freq, freq2) =>
                    freq.Concat(freq2)
                        .GroupBy(t => t.Item1)
                        .Select(gp => new Tuple<string, int>(gp.Key, gp.Select(t => t.Item2).Sum()))
                        .ToArray();

            var workflow = CloudBuilder.MapReduce(mapper, reducer, new Tuple<string, int>[] { }, texts);
            var results = this.Run(workflow);
            var expected = mapper.Invoke(String.Join(",", texts));
            Assert.AreEqual(expected, results);
        }
    }
}
