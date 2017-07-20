using System;
using System.Threading.Tasks;
using System.IO;
using System.Threading;
using CRA.ClientLibrary;
using System.Diagnostics;

namespace BandwidthTest
{
    public class MyAsyncInput : IAsyncProcessInputEndpoint
    {
        bool _running = true;
        IProcess _process;
        int _chunkSize;
        byte[] _dataset;

        public MyAsyncInput(IProcess process, int chunkSize)
        {
            _process = process;
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

        public async Task FromStreamAsync(Stream stream, string otherProcess, string otherEndpoint, CancellationToken token)
        {
            Console.WriteLine("Receiving data from process: " + otherProcess + ", endpoint: " + otherEndpoint);
            int bwCheckPeriod = 1000 * 1024 * 1024 / _chunkSize;

            Stopwatch sw = new Stopwatch();
            sw.Start();

            for (int i = 0; i < int.MaxValue; i++)
            {
                await stream.ReadAllRequiredBytesAsync(_dataset, 0, _chunkSize);
                if ((i > 0) && (i % bwCheckPeriod == 0))
                {
                    Console.WriteLine("Incoming bandwidth from process {0}, endpoint {1} = {2} MBps", otherProcess, otherEndpoint, _chunkSize * (double)bwCheckPeriod / (1000 * (double)sw.ElapsedMilliseconds));
                    sw.Restart();
                }
                if (!_running) break;
            }
        }
    }

}
