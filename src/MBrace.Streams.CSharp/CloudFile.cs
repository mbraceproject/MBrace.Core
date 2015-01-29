using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MBrace.Streams.CSharp
{
    internal class LineEnumerator : IEnumerator<string>
    {
        StreamReader reader;
        string current;

        public LineEnumerator(System.IO.Stream stream)
        {
            this.reader = new StreamReader(stream);
        }

        public string Current
        {
            get { return current; }
        }

        public void Dispose()
        {
            reader.Dispose();
        }

        object System.Collections.IEnumerator.Current
        {
            get { return current; }
        }

        public bool MoveNext()
        {
            if (reader.EndOfStream) return false;
            else
            {
                current = reader.ReadLine();
                return true;
            }
        }

        public void Reset()
        {
            reader.BaseStream.Seek(0L, SeekOrigin.Begin);
        }

    }

    internal class LineEnumerable : IEnumerable<string>
    {
        System.IO.Stream stream;

        public LineEnumerable(System.IO.Stream stream)
        {
            this.stream = stream;
        }

        public IEnumerator<string> GetEnumerator()
        {
            return new LineEnumerator(stream);
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return new LineEnumerator(stream);
        }
    }

    /// <summary>
    /// Common CloudFile readers.
    /// </summary>
    public static class CloudFileReader
    {
        /// <summary>
        /// Read file as a string.
        /// </summary>
        public static Func<System.IO.Stream, Task<string>> ReadAllText =
                async stream =>
                {
                    using (var sr = new StreamReader(stream))
                        return await sr.ReadToEndAsync();
                };

        /// <summary>
        /// Lazily read all lines.
        /// </summary>
        public static Func<System.IO.Stream, Task<IEnumerable<string>>> ReadLines =
                stream => Task.FromResult<IEnumerable<string>>(new LineEnumerable(stream));

        /// <summary>
        /// Read all lines.
        /// </summary>
        public static Func<System.IO.Stream, Task<string[]>> ReadAllLines =
                async stream =>
                {
                    var lines = new List<string>();
                    using (var sr = new StreamReader(stream))
                        while (!sr.EndOfStream)
                            lines.Add(await sr.ReadLineAsync());
                    return lines.ToArray();
                };

        /// <summary>
        /// Read all bytes.
        /// </summary>
        public static Func<System.IO.Stream, Task<byte[]>> ReadAllBytes =
                async stream =>
                {
                    using (var ms = new MemoryStream())
                    {
                        await stream.CopyToAsync(ms);
                        return ms.ToArray();
                    }
                };
    }

}
