using CRA.ClientLibrary;

namespace ConnectionPair
{
    public class MyFirstProcess : ProcessBase
    {
        public MyFirstProcess() : base()
        {
        }

        public override void Initialize(object param)
        {
            AddAsyncOutputEndpoint("firstoutput", new MyAsyncOutput(this));
            AddAsyncInputEndpoint("firstinput", new MyAsyncInput(this));
            base.Initialize(param);
        }
    }
}
