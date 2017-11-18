using System;
using System.Threading.Tasks;
using System.IO;
using System.Threading;
using CRA.ClientLibrary;

namespace ConnectionPair
{
    public class MyAsyncInput : IAsyncVertexInputEndpoint
    {
        bool _running = true;
        IVertex _vertex;

        public MyAsyncInput(IVertex vertex)
        {
            _vertex = vertex;
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

        public async Task FromStreamAsync(Stream stream, string otherVertex, string otherEndpoint, CancellationToken token)
        {
            Console.WriteLine("Receiving data from vertex: " + otherVertex + ", endpoint: " + otherEndpoint);

            for (int i = 0; i < int.MaxValue; i++)
            {
                int val = stream.ReadInt32();
                Console.WriteLine("Read value: " + val);
                token.ThrowIfCancellationRequested();

                if (!_running) break;
            }
        }
    }

}
