using System;
using System.Threading.Tasks;
using System.IO;
using System.Threading;
using CRA.ClientLibrary;

namespace ShardedConnectionPair
{
    public class MyAsyncInput : AsyncShardedVertexInputEndpointBase
    {
        bool _running = true;
        IVertex _vertex;

        public MyAsyncInput(IVertex vertex)
        {
            _vertex = vertex;
        }

        public override void Dispose()
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

        public override async Task FromStreamAsync(Stream stream, string otherVertex, int otherShardId, string otherEndpoint, CancellationToken token)
        {
            Console.WriteLine("UPDATE!!!!!!!!!!!!!!!");

            Console.WriteLine("Receiving data from vertex: " + otherVertex + ", endpoint: " + otherEndpoint);

            for (int i = 0; i < int.MaxValue; i++)
            {
                int val = stream.ReadInt32();
                Console.WriteLine("AARead value: " + val);
                token.ThrowIfCancellationRequested();

                if (!_running) break;
            }
        }
    }
}
