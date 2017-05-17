using CRA.ClientLibrary;

namespace ConnectionPair
{
    public class ConnectionPairProcess : ProcessBase
    {
        public ConnectionPairProcess() : base()
        {
        }

        public override void Initialize(object param)
        {
            AddAsyncInputEndpoint("input", new MyAsyncInput(this));
            AddAsyncOutputEndpoint("output", new MyAsyncOutput(this));
            base.Initialize(param);
        }
    }
}
