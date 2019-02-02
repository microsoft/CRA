using System;
using System.Threading.Tasks;
using System.IO;
using System.Threading;
using CRA.ClientLibrary;
using System.Diagnostics;

namespace BandwidthTest
{
    public class MyAsyncInput : IAsyncVertexInputEndpoint
    {
        bool _running = true;
        IVertex _vertex;
        int _chunkSize;
        byte[] _dataset;

        public MyAsyncInput(IVertex vertex, int chunkSize)
        {
            _vertex = vertex;
            _chunkSize = chunkSize;
            _dataset = new byte[chunkSize];
        }

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                Console.WriteLine("Disposing MyInput");
                _running = false;
            }
        }

        public async Task FromStreamAsync(
            Stream stream,
            string otherVertex,
            string otherEndpoint,
            CancellationToken token)
        {
            Console.WriteLine("Receiving data from vertex: " + otherVertex + ", endpoint: " + otherEndpoint);
            int bwCheckPeriod = 1000 * 1024 * 1024 / _chunkSize;

            Stopwatch sw = new Stopwatch();
            sw.Start();

            for (int i = 0; i < int.MaxValue; i++)
            {
                await stream.ReadAllRequiredBytesAsync(_dataset, 0, _chunkSize);
                if ((i > 0) && (i % bwCheckPeriod == 0))
                {
                    Console.WriteLine("Incoming bandwidth from vertex {0}, endpoint {1} = {2} MBps", otherVertex, otherEndpoint, _chunkSize * (double)bwCheckPeriod / (1000 * (double)sw.ElapsedMilliseconds));
                    sw.Restart();
                }

                if (!_running) break;
            }
        }
    }

}
