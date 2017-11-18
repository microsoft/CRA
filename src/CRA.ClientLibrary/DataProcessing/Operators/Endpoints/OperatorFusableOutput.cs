using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace CRA.ClientLibrary.DataProcessing
{
    public class OperatorFusableOutput : IAsyncFusableVertexOutputEndpoint
    {
        protected OperatorBase _operator;
        protected int _thisId;
        private OperatorFusableInput _inputEndpoint;

        public OperatorFusableInput InputEndpoint { get { return _inputEndpoint; } } 

        public OperatorFusableOutput(ref IVertex vertex, int thisId)
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
            throw new NotImplementedException();
        }

        public bool CanFuseWith(IAsyncVertexInputEndpoint endpoint, string otherVertex, string otherEndpoint)
        {
            if (endpoint as OperatorFusableInput != null) return true;
            return false;
        }

        public async Task ToInputAsync(IAsyncVertexInputEndpoint inputEndpoint, string otherVertex, string otherEndpoint, CancellationToken token)
        {
            _inputEndpoint = inputEndpoint as OperatorFusableInput;
            bool isAdded = await _inputEndpoint.AddOperatorFusableInput();

            if (isAdded)
            {
                IEndpointContent endpointContent = new ObjectEndpoint();
                ((ObjectEndpoint)endpointContent).SetOperatorOutputEndpoint(this);

                _operator.AddOutput(_thisId, ref endpointContent);
                _operator.WaitForOutputCompletion(_thisId);
            }
        }

    }
}
