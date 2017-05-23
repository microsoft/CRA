using System;
using System.Threading.Tasks;
using System.IO;
using System.Threading;
using CRA.ClientLibrary;

namespace ConnectionPair
{
    public class MyAsyncOutput : IAsyncProcessOutputEndpoint
    {
        bool _running = true;
        IProcess _process;

        public MyAsyncOutput(IProcess process)
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

        public async Task ToInputAsync(IProcessInputEndpoint endpoint, CancellationToken token)
        {
            throw new NotImplementedException();
        }

        public async Task ToStreamAsync(Stream stream, string otherProcess, string otherEndpoint, CancellationToken token)
        {
            Console.WriteLine("Sending data to process: " + otherProcess + ", endpoint: " + otherEndpoint);

            for (int i = 0; i < int.MaxValue; i += 1)
            {
                for (int j = 0; j < 1; j++)
                {
                    stream.WriteInteger(i + j);
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
