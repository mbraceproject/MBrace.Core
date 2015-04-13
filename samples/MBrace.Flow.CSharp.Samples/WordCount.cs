using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Nessos.Streams.CSharp;
using MBrace.Flow.CSharp;
using System.Text.RegularExpressions;
using MBrace.SampleRuntime;

namespace MBrace.Flow.CSharp.Samples
{
    static class WordCount
    {
        static string[] files;

        #region NoiseWords
        static HashSet<string> noiseWords = 
            new HashSet<string> {
                "a", "about", "above", "all", "along", "also", "although", "am", "an", "any", "are", "aren't", "as", "at",
                "be", "because", "been", "but", "by", "can", "cannot", "could", "couldn't", "did", "didn't", "do", "does", 
                "doesn't", "e.g.", "either", "etc", "etc.", "even", "ever","for", "from", "further", "get", "gets", "got", 
                "had", "hardly", "has", "hasn't", "having", "he", "hence", "her", "here", "hereby", "herein", "hereof", 
                "hereon", "hereto", "herewith", "him", "his", "how", "however", "I", "i.e.", "if", "into", "it", "it's", "its",
                "me", "more", "most", "mr", "my", "near", "nor", "now", "of", "onto", "other", "our", "out", "over", "really", 
                "said", "same", "she", "should", "shouldn't", "since", "so", "some", "such", "than", "that", "the", "their", 
                "them", "then", "there", "thereby", "therefore", "therefrom", "therein", "thereof", "thereon", "thereto", 
                "therewith", "these", "they", "this", "those", "through", "thus", "to", "too", "under", "until", "unto", "upon",
                "us", "very", "viz", "was", "wasn't", "we", "were", "what", "when", "where", "whereby", "wherein", "whether",
                "which", "while", "who", "whom", "whose", "why", "with", "without", "would", "you", "your" , "have", "thou", "will", 
                "shall"
            }; 
        #endregion

        #region Helper functions
        static string[] SplitWords(this string text)
        {
            var regex = new Regex(@"[\W]+", RegexOptions.Compiled);
            return regex.Split(text);
        }

        static string WordTransform(this string word)
        {
            return word.Trim().ToLower();
        }

        static bool WordFilter(this string word)
        {
            return word.Length > 3 && !noiseWords.Contains(word);
        } 
        #endregion

        public static string FilesPath { set { files = Directory.GetFiles(value); } } 

        public static IEnumerable<Tuple<string,long>> RunWithCloudFiles(MBraceRuntime runtime)
        {
            //var cfiles = runtime.StoreClient.Default.UploadFiles(files, "wordcount");
            IEnumerable<CloudFile> cfiles = files.Select(path => {
                var text = File.ReadAllText(path);
                return runtime.StoreClient.FileStore.File.WriteAllText(text, null, null);
            }).ToArray();

            var count = 20;
    
            var query = cfiles
                            .AsCloudFlow(CloudFileReader.ReadAllLines)
                            .SelectMany(lines => lines)
                            .SelectMany(line => line.SplitWords().Select(WordTransform))
                            .Where(WordFilter)
                            .CountBy(w => w)
                            .OrderBy(t => -t.Item2, count) 
                            .ToArray();

            // Temporary : No C# API for Standalone runtime and StoreClient.
            var result = runtime.Run(query, null, null);
            return result;
        }

        public static IEnumerable<Tuple<string, long>> RunWithCloudVector(MBraceRuntime runtime)
        {
            var lines = files.SelectMany(path => File.ReadLines(path));
            // Temporary : No C# API for Standalone runtime and StoreClient.
            var vector = runtime.RunLocally(CloudVector.New(lines, 100L), null);
            
            var count = 20;

            var query = vector
                            .AsCloudFlow()
                            .SelectMany(line => line.SplitWords().Select(WordTransform))
                            .Where(WordFilter)
                            .CountBy(w => w)
                            .OrderBy(t => -t.Item2, count)
                            .ToArray();

            var result = runtime.Run(query, null, null);
            return result;
        }
    }
}
