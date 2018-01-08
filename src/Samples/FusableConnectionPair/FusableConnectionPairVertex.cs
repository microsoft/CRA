using CRA.ClientLibrary;

namespace FusableConnectionPair
{
    public class FusableConnectionPairVertex : VertexBase
    {
        public FusableConnectionPairVertex() : base()
        {
        }

        public override void Initialize(object vertexParameter)
        {
            AddAsyncInputEndpoint("input", new MyAsyncFusableInput(this));
            AddAsyncOutputEndpoint("output", new MyAsyncFusableOutput(this));
            base.Initialize(vertexParameter);
        }
    }
}
