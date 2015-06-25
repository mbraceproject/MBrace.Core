using MBrace.Thespian;
using MBrace.Streams.CSharp;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace MBrace.Flow.CSharp.Samples
{
    class Program
    {
        static void Main(string[] args)
        {
            var files = Directory.GetFiles("path to your files");

            var path = Path.GetDirectoryName(Assembly.GetEntryAssembly().Location);
            var mbraced = Path.Combine(path, @"./MBrace.Thespian.exe");
            MBraceThespian.WorkerExecutable = mbraced;
            var runtime = MBraceThespian.InitLocal(4, null, null);

            
            WordCount.FilesPath = @"path to your files";

            var top1 = WordCount.RunWithCloudFiles(runtime);
            //var top2 = WordCount.RunWithCloudArray(runtime);

            runtime.KillAllWorkers();
        }
    }
}
