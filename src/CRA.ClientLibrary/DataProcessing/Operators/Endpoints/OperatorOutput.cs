using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace CRA.ClientLibrary.DataProcessing
{
    public class OperatorOutput : IAsyncProcessOutputEndpoint
    {
        protected OperatorBase _operator;
        protected int _thisId;

        public OperatorOutput(ref IProcess process, int thisId)
        {
            _operator = (OperatorBase)process;
            _thisId = thisId;
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
                Console.WriteLine("Disposing OperatorOutput");
            }
        }

        public async Task ToStreamAsync(Stream stream, string otherProcess, string otherEndpoint, CancellationToken token)
        {
            IEndpointContent streamEndpoint = new StreamEndpoint(stream);
            _operator.AddOutput(_thisId, ref streamEndpoint);
            _operator.WaitForOutputCompletion(_thisId);
        }
    }
}
