using System;
using System.Threading.Tasks;
using System.IO;
using System.Threading;
using CRA.ClientLibrary;

namespace BandwidthTest
{
    public class MyAsyncOutput : IAsyncProcessOutputEndpoint
    {
        bool _running = true;
        IProcess _process;
        byte[] _dataset;
        int _chunkSize;

        public MyAsyncOutput(IProcess process, int chunkSize)
        {
            _process = process;
            _chunkSize = chunkSize;
            _dataset = new byte[chunkSize];
            Random r = new Random();
            r.NextBytes(_dataset);
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
                Console.WriteLine("Disposing MyOutput");
                _running = false;
            }
        }

        public async Task ToInputAsync(IProcessInputEndpoint endpoint, CancellationToken token)
        {
            throw new NotImplementedException();
        }

        public async Task ToStreamAsync(Stream stream, string otherProcess, string otherEndpoint, CancellationToken token)
        {
            Console.WriteLine("Sending data to process: " + otherProcess + ", endpoint: " + otherEndpoint);
            for (int i = 0; i < int.MaxValue; i += 1)
            {
                await stream.WriteAsync(_dataset, 0, _chunkSize);
                if (!_running) break;
            }
        }
    }

}
