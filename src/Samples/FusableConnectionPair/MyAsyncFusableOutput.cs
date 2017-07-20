using System;
using System.Threading.Tasks;
using System.IO;
using System.Threading;
using CRA.ClientLibrary;

namespace FusableConnectionPair
{
    public class MyAsyncFusableOutput : IAsyncFusableProcessOutputEndpoint
    {
        bool _running = true;
        IProcess _process;

        public MyAsyncFusableOutput(IProcess process)
        {
            _process = process;
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

        public async Task ToStreamAsync(Stream stream, string otherProcess, string otherEndpoint, CancellationToken token)
        {
            Console.WriteLine("Sending data to process: " + otherProcess + ", endpoint: " + otherEndpoint);

            for (int i = 0; i < int.MaxValue; i += 1)
            {
                for (int j = 0; j < 1; j++)
                {
                    stream.WriteInt32(i + j);
                    Console.WriteLine("Written value: " + (i + j));
                }


                for (int j = 0; j < 1; j++)
                {
                    Thread.Sleep(1000);
                    token.ThrowIfCancellationRequested();
                }

                if (!_running) break;
            }
        }

        public bool CanFuseWith(IAsyncProcessInputEndpoint endpoint, string otherProcess, string otherEndpoint)
        {
            if (endpoint as MyAsyncFusableInput != null) return true;
            return false;
        }

        public async Task ToInputAsync(IAsyncProcessInputEndpoint endpoint, string otherProcess, string otherEndpoint, CancellationToken token)
        {
            Console.WriteLine("Sending data to fused process: " + otherProcess + ", endpoint: " + otherEndpoint);

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
                    Thread.Sleep(1000);
                    token.ThrowIfCancellationRequested();
                }

                if (!_running) break;
            }
        }
    }
}
