using CRA.ClientLibrary;

namespace ConnectionPair
{
    public class ConnectionPairVertex : VertexBase
    {
        public ConnectionPairVertex() : base()
        {
        }

        public override void Initialize(object vertexParameter)
        {
            AddAsyncInputEndpoint("input", new MyAsyncInput(this));
            AddAsyncOutputEndpoint("output", new MyAsyncOutput(this));
            base.Initialize(vertexParameter);
        }
    }
}
