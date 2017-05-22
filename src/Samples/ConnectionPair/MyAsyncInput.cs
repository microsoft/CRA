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
            Console.WriteLine("Disposing MyInput");
            _running = false;
        }

        public async Task FromOutputAsync(IProcessOutputEndpoint p, CancellationToken token)
        {
            throw new NotImplementedException();
        }

        public async Task FromStreamAsync(Stream s, string otherProcess, string otherEndpoint, CancellationToken token)
        {
            Console.WriteLine("Receiving data from process: " + otherProcess + ", endpoint: " + otherEndpoint);

            for (int i = 0; i < int.MaxValue; i++)
            {
                int val = s.ReadInt32();
                Console.WriteLine("Read value: " + val);
                token.ThrowIfCancellationRequested();

                if (!_running) break;
            }
        }
    }

}
