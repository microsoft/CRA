using System;
using System.Threading.Tasks;
using System.IO;
using System.Threading;
using CRA.ClientLibrary;

namespace ConnectionPair
{
    public class MyAsyncInput : IAsyncProcessInputEndpoint
    {
        bool _running = true;
        IProcess _process;

        public MyAsyncInput(IProcess process)
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
                Console.WriteLine("Disposing MyInput");
                _running = false;
            }
        }

        public async Task FromOutputAsync(IProcessOutputEndpoint endpoint, CancellationToken token)
        {
            throw new NotImplementedException();
        }

        public async Task FromStreamAsync(Stream stream, string otherProcess, string otherEndpoint, CancellationToken token)
        {
            Console.WriteLine("Receiving data from process: " + otherProcess + ", endpoint: " + otherEndpoint);

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
