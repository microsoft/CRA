using System.Runtime.Serialization;

namespace CRA.ClientLibrary.DataProcessing
{
    [DataContract]
    public class SubscribeTask : TaskBase
    {
        public SubscribeTask() : base(OperatorType.Subscribe){}
    }
}
