using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace CRA.ClientLibrary.DataProcessing
{
    public class OperatorOutput : IAsyncVertexOutputEndpoint
    {
        protected OperatorBase _operator;
        protected int _thisId;

        public OperatorOutput(ref IVertex vertex, int thisId)
        {
            _operator = (OperatorBase)vertex;
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

        public async Task ToStreamAsync(Stream stream, string otherVertex, string otherEndpoint, CancellationToken token)
        {
            IEndpointContent streamEndpoint = new StreamEndpoint(stream);
            _operator.AddOutput(_thisId, ref streamEndpoint);
            _operator.WaitForOutputCompletion(_thisId);
        }
    }
}
