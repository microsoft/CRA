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

        public OperatorOutput(IProcess process, int thisId)
        {
            _operator = (OperatorBase)process;
            _thisId = thisId;
        }

        public void Dispose()
        {
            Console.WriteLine("Disposing OperatorOutput");
        }

        public Task ToInputAsync(IProcessInputEndpoint endpoint, CancellationToken token)
        {
            throw new NotImplementedException();
        }

        public async Task ToStreamAsync(Stream stream, string otherProcess, string otherEndpoint, CancellationToken token)
        {
            _operator.AddOutput(_thisId, stream);
            _operator.WaitForOutputCompletion(_thisId);
            _operator.RemoveOutput(_thisId);
        }
    }
}
