using System.Runtime.Serialization;

namespace CRA.ClientLibrary.DataProcessing
{
    [DataContract]
    public class ProduceTask : TaskBase
    {
        [DataMember]
        private string _producer;

        public string DataProducer { get { return _producer; } }

        public ProduceTask(string producer) : base(OperatorType.Produce)
        {
            _producer = producer;
        }

    }
}
