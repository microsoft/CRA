using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace CRA.ClientLibrary.DataProcessing
{
    public class OperatorFusableInput : IAsyncProcessInputEndpoint
    {
        protected OperatorBase _operator;
        protected int _thisId;
        private bool _isSecondaryInput = false;

        public object Dataset { get; set; }

        public ObjectEndpoint EndpointContent { get; set; }

        public OperatorFusableInput(ref IProcess process, int thisId, bool isSecondaryInput = false)
        {
            _operator = (OperatorBase)process;
            _thisId = thisId;
            _isSecondaryInput = isSecondaryInput;
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
                Console.WriteLine("Disposing OperatorInput");
            }
        }

        public async Task FromStreamAsync(Stream stream, string otherProcess, string otherEndpoint, CancellationToken token)
        {
            throw new NotImplementedException();
        }

        public async Task<bool> AddOperatorFusableInput()
        {
            IEndpointContent endpointContent = new ObjectEndpoint();
            ((ObjectEndpoint)endpointContent).SetOperatorInputEndpoint(this);
            EndpointContent = (ObjectEndpoint)endpointContent;
            Task.Run(() => AddOperatorInput(EndpointContent));
            return true;
        }

        public async void AddOperatorInput(IEndpointContent endpointContent)
        {
            if (_isSecondaryInput)
            {
                _operator.AddSecondaryInput(_thisId, ref endpointContent);
                _operator.WaitForSecondaryInputCompletion(_thisId);
            }
            else
            {
                _operator.AddInput(_thisId, ref endpointContent);
                _operator.WaitForInputCompletion(_thisId);
            }
        }
    }
}
