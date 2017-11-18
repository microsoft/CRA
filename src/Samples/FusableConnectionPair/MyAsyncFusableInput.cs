using System;
using System.Threading.Tasks;
using System.IO;
using System.Threading;
using CRA.ClientLibrary;

namespace FusableConnectionPair
{
    public class MyAsyncFusableInput : IAsyncVertexInputEndpoint
    {
        bool _running = true;
        IVertex _vertex;

        public MyAsyncFusableInput(IVertex vertex)
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

        public void WriteInt32(int val)
        {
            Console.WriteLine("Read value: " + val);
        }
    }

}
