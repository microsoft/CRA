using System;
using System.Threading.Tasks;
using System.IO;
using System.Threading;
using CRA.ClientLibrary;

namespace FusableConnectionPair
{
    public class MyAsyncFusableOutput : IAsyncFusableVertexOutputEndpoint
    {
        bool _running = true;
        IVertex _vertex;

        public MyAsyncFusableOutput(IVertex vertex)
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
                Console.WriteLine("Disposing MyOutput");
                _running = false;
            }
        }

        public async Task ToStreamAsync(Stream stream, string otherVertex, string otherEndpoint, CancellationToken token)
        {
            Console.WriteLine("Sending data to vertex: " + otherVertex + ", endpoint: " + otherEndpoint);

            for (int i = 0; i < int.MaxValue; i += 1)
            {
                for (int j = 0; j < 1; j++)
                {
                    stream.WriteInt32(i + j);
                    Console.WriteLine("Written value: " + (i + j));
                }


                for (int j = 0; j < 1; j++)
                {
                    await Task.Delay(1000);
                    token.ThrowIfCancellationRequested();
                }

                if (!_running) break;
            }
        }

        public bool CanFuseWith(IAsyncVertexInputEndpoint endpoint, string otherVertex, string otherEndpoint)
        {
            if (endpoint as MyAsyncFusableInput != null) return true;
            return false;
        }

        public async Task ToInputAsync(IAsyncVertexInputEndpoint endpoint, string otherVertex, string otherEndpoint, CancellationToken token)
        {
            Console.WriteLine("Sending data to fused vertex: " + otherVertex + ", endpoint: " + otherEndpoint);

            MyAsyncFusableInput otherInstance = endpoint as MyAsyncFusableInput;

            for (int i = 0; i < int.MaxValue; i += 1)
            {
                for (int j = 0; j < 1; j++)
                {
                    otherInstance.WriteInt32(i + j);
                    Console.WriteLine("Written value: " + (i + j));
                }


                for (int j = 0; j < 1; j++)
                {
                    await Task.Delay(1000);
                    token.ThrowIfCancellationRequested();
                }

                if (!_running) break;
            }
        }
    }
}
