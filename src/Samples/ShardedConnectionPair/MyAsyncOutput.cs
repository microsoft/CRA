using System;
using System.Threading.Tasks;
using System.IO;
using System.Threading;
using CRA.ClientLibrary;

namespace ShardedConnectionPair
{
    public class MyAsyncOutput : AsyncShardedVertexOutputEndpointBase
    {
        bool _running = true;
        IVertex _vertex;

        public MyAsyncOutput(IVertex vertex)
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
                Console.WriteLine("Disposing MyOutput");
                _running = false;
            }
        }

        public override async Task ToStreamAsync(Stream stream, string otherVertex, int otherShardId, string otherEndpoint, CancellationToken token)
        {
            Console.WriteLine("Sending data to vertex: " + otherVertex + ", endpoint: " + otherEndpoint);

            for (int i = 0; i < int.MaxValue; i += 1)
            {
                for (int j = 0; j < 1; j++)
                {
                    stream.WriteInt32(i + j);
                    Console.WriteLine("AAWritten value: " + (i + j));
                }


                for (int j = 0; j < 1; j++)
                {
                    Thread.Sleep(1000);
                    token.ThrowIfCancellationRequested();
                }

                if (!_running) break;
            }
        }
    }
}
