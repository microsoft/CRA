using CRA.ClientLibrary;

namespace BandwidthTest
{
    public class BandwidthTestProcess : ProcessBase
    {
        private int _chunkSize;

        public BandwidthTestProcess() : base()
        {
        }

        public override void Initialize(object processParameter)
        {
            _chunkSize = (int)processParameter;
            AddAsyncInputEndpoint("input1", new MyAsyncInput(this, _chunkSize));
            AddAsyncOutputEndpoint("output1", new MyAsyncOutput(this, _chunkSize));
            AddAsyncInputEndpoint("input2", new MyAsyncInput(this, _chunkSize));
            AddAsyncOutputEndpoint("output2", new MyAsyncOutput(this, _chunkSize));
            AddAsyncInputEndpoint("input3", new MyAsyncInput(this, _chunkSize));
            AddAsyncOutputEndpoint("output3", new MyAsyncOutput(this, _chunkSize));
            base.Initialize(processParameter);
        }
    }
}
