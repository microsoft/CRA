using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace CRA.ClientLibrary.DataProcessing
{
    public class OperatorInput : IAsyncProcessInputEndpoint
    {
        protected OperatorBase _operator;
        protected int _thisId;
        private bool _isSecondaryInput = false;

        public OperatorInput(IProcess process, int thisId, bool isSecondaryInput = false)
        {
            _operator = (OperatorBase)process;
            _thisId = thisId;
            _isSecondaryInput = isSecondaryInput;
        }

        public void Dispose()
        {
            Console.WriteLine("Disposing OperatorInput");
        }

        public Task FromOutputAsync(IProcessOutputEndpoint endpoint, CancellationToken token)
        {
            throw new NotImplementedException();
        }

        public async Task FromStreamAsync(Stream stream, string otherProcess, string otherEndpoint, CancellationToken token)
        {
            if (_isSecondaryInput)
            {
                _operator.AddSecondaryInput(_thisId, stream);
                _operator.WaitForSecondaryInputCompletion(_thisId);
                _operator.RemoveSecondaryInput(_thisId);
            }
            else
            {
                _operator.AddInput(_thisId, stream);
                _operator.WaitForInputCompletion(_thisId);
                _operator.RemoveInput(_thisId);
            }
        }
    }
}
