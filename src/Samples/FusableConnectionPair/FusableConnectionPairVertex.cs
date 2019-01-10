using CRA.ClientLibrary;
using System.Threading.Tasks;

namespace FusableConnectionPair
{
    public class FusableConnectionPairVertex : VertexBase
    {
        public FusableConnectionPairVertex() : base()
        {
        }

        public override Task InitializeAsync(object vertexParameter)
        {
            AddAsyncInputEndpoint("input", new MyAsyncFusableInput(this));
            AddAsyncOutputEndpoint("output", new MyAsyncFusableOutput(this));
            return base.InitializeAsync(vertexParameter);
        }
    }
}
