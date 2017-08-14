using System.Threading;
using System.Threading.Tasks;

namespace CRA.ClientLibrary.DataProcessing
{
    public class ObjectEndpoint : IEndpointContent
    {
        private OperatorFusableInput _owningInputEndpoint;

        public OperatorFusableInput OwningInputEndpoint
        {
            get { return _owningInputEndpoint; }
        }

        private OperatorFusableOutput _owningOutputEndpoint;

        public OperatorFusableOutput OwningOutputEndpoint
        {
            get { return _owningOutputEndpoint; }
        }

        public ManualResetEvent ReadyTrigger { get; set; }

        public ManualResetEvent FireTrigger { get; set; }

        public ManualResetEvent ReleaseTrigger { get; set; }

        public object Dataset { get; set; }

        public ObjectEndpoint()
        {
            ReadyTrigger = new ManualResetEvent(false);
            FireTrigger = new ManualResetEvent(false);
            ReleaseTrigger = new ManualResetEvent(false);
        }

        public void SetOperatorInputEndpoint(OperatorFusableInput owningInputEndpoint)
        {
            _owningInputEndpoint = owningInputEndpoint;
        }

        public void SetOperatorOutputEndpoint(OperatorFusableOutput owningOutputEndpoint)
        {
            _owningOutputEndpoint = owningOutputEndpoint;
        }

        internal async Task<bool> OnReceivedReadyMessage()
        {
            return ReadyTrigger.WaitOne();
        }


        internal bool OnReceivedFireMessage()
        {
            return FireTrigger.WaitOne();
        }

        internal async Task<bool> OnReceivedReleaseMessage()
        {
            return ReleaseTrigger.WaitOne();
        }
    }
}
