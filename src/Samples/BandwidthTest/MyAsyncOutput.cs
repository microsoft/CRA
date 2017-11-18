using System;
using System.Threading.Tasks;
using System.IO;
using System.Threading;
using CRA.ClientLibrary;

namespace BandwidthTest
{
    public class MyAsyncOutput : IAsyncVertexOutputEndpoint
    {
        bool _running = true;
        IVertex _vertex;
        byte[] _dataset;
        int _chunkSize;

        public MyAsyncOutput(IVertex vertex, int chunkSize)
        {
            _vertex = vertex;
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

        public async Task ToStreamAsync(Stream stream, string otherVertex, string otherEndpoint, CancellationToken token)
        {
            Console.WriteLine("Sending data to vertex: " + otherVertex + ", endpoint: " + otherEndpoint);
            for (int i = 0; i < int.MaxValue; i += 1)
            {
                await stream.WriteAsync(_dataset, 0, _chunkSize);
                if (!_running) break;
            }
        }
    }

}
