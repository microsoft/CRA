using CRA.ClientLibrary;
using System.Threading.Tasks;

namespace BandwidthTest
{
    public class BandwidthTestVertex : VertexBase
    {
        private int _chunkSize;

        public BandwidthTestVertex() : base()
        {
        }

        public override Task InitializeAsync(object vertexParameter)
        {
            _chunkSize = (int)vertexParameter;
            AddAsyncInputEndpoint("input1", new MyAsyncInput(this, _chunkSize));
            AddAsyncOutputEndpoint("output1", new MyAsyncOutput(this, _chunkSize));
            AddAsyncInputEndpoint("input2", new MyAsyncInput(this, _chunkSize));
            AddAsyncOutputEndpoint("output2", new MyAsyncOutput(this, _chunkSize));
            AddAsyncInputEndpoint("input3", new MyAsyncInput(this, _chunkSize));
            AddAsyncOutputEndpoint("output3", new MyAsyncOutput(this, _chunkSize));
            AddAsyncInputEndpoint("input4", new MyAsyncInput(this, _chunkSize));
            AddAsyncOutputEndpoint("output4", new MyAsyncOutput(this, _chunkSize));
            AddAsyncInputEndpoint("input5", new MyAsyncInput(this, _chunkSize));
            AddAsyncOutputEndpoint("output5", new MyAsyncOutput(this, _chunkSize));
            AddAsyncInputEndpoint("input6", new MyAsyncInput(this, _chunkSize));
            AddAsyncOutputEndpoint("output6", new MyAsyncOutput(this, _chunkSize));
            AddAsyncInputEndpoint("input7", new MyAsyncInput(this, _chunkSize));
            AddAsyncOutputEndpoint("output7", new MyAsyncOutput(this, _chunkSize));
            AddAsyncInputEndpoint("input8", new MyAsyncInput(this, _chunkSize));
            AddAsyncOutputEndpoint("output8", new MyAsyncOutput(this, _chunkSize));
            AddAsyncInputEndpoint("input9", new MyAsyncInput(this, _chunkSize));
            AddAsyncOutputEndpoint("output9", new MyAsyncOutput(this, _chunkSize));
            AddAsyncInputEndpoint("input10", new MyAsyncInput(this, _chunkSize));
            AddAsyncOutputEndpoint("output10", new MyAsyncOutput(this, _chunkSize));
            AddAsyncInputEndpoint("input11", new MyAsyncInput(this, _chunkSize));
            AddAsyncOutputEndpoint("output11", new MyAsyncOutput(this, _chunkSize));
            AddAsyncInputEndpoint("input12", new MyAsyncInput(this, _chunkSize));
            AddAsyncOutputEndpoint("output12", new MyAsyncOutput(this, _chunkSize));
            AddAsyncInputEndpoint("input13", new MyAsyncInput(this, _chunkSize));
            AddAsyncOutputEndpoint("output13", new MyAsyncOutput(this, _chunkSize));
            AddAsyncInputEndpoint("input14", new MyAsyncInput(this, _chunkSize));
            AddAsyncOutputEndpoint("output14", new MyAsyncOutput(this, _chunkSize));
            AddAsyncInputEndpoint("input15", new MyAsyncInput(this, _chunkSize));
            AddAsyncOutputEndpoint("output15", new MyAsyncOutput(this, _chunkSize));
            AddAsyncInputEndpoint("input16", new MyAsyncInput(this, _chunkSize));
            AddAsyncOutputEndpoint("output16", new MyAsyncOutput(this, _chunkSize));

            return base.InitializeAsync(vertexParameter);
        }
    }
}
