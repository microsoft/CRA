using CRA.ClientLibrary;
using System.Threading.Tasks;

namespace ConnectionPair
{
    public class ConnectionPairVertex : VertexBase
    {
        public ConnectionPairVertex() : base()
        {
        }

        public override Task InitializeAsync(object vertexParameter)
        {
            AddAsyncInputEndpoint("input", new MyAsyncInput(this));
            AddAsyncOutputEndpoint("output", new MyAsyncOutput(this));
            return base.InitializeAsync(vertexParameter);
        }
    }
}
