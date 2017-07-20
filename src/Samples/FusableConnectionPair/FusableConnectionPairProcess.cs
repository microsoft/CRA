using CRA.ClientLibrary;

namespace FusableConnectionPair
{
    public class FusableConnectionPairProcess : ProcessBase
    {
        public FusableConnectionPairProcess() : base()
        {
        }

        public override void Initialize(object processParameter)
        {
            AddAsyncInputEndpoint("input", new MyAsyncFusableInput(this));
            AddAsyncOutputEndpoint("output", new MyAsyncFusableOutput(this));
            base.Initialize(processParameter);
        }
    }
}
