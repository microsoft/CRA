using CRA.ClientLibrary;

namespace ConnectionPair
{
    public class MySecondProcess : ProcessBase
    {
        public MySecondProcess() : base()
        {
        }
        public override void Initialize(object param)
        {
            AddAsyncInputEndpoint("secondinput", new MyAsyncInput(this));
            AddAsyncOutputEndpoint("secondoutput", new MyAsyncOutput(this));
            base.Initialize(param);
        }
    }

}
