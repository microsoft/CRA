using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.Serialization;

namespace CRA.ClientLibrary.DataProcessing
{
    [DataContract]
    public class TaskBase
    {
        [DataMember]
        private static int TaskIdSequence = 0;

        [DataMember]
        private int _taskId;

        [DataMember]
        private OperatorType _operationType;

        internal int TaskId { get { return _taskId; } }

        internal OperatorType OperationType { get { return _operationType;  } }

        [DataMember]
        internal bool IsRightOperandInput { get; set; }

        [DataMember]
        internal BinaryOperatorTypes OperationTypes { get; set; }

        [DataMember]
        internal OperatorInputs InputIds { get; set; }

        [DataMember]
        internal OperatorInputs NextInputIds { get; set; }

        [DataMember]
        internal string OutputId { get; set; }

        [DataMember]
        internal string[] Transforms { get; set; }

        [DataMember]
        internal string[] TransformsOperations { get; set; }

        [DataMember]
        internal string[] TransformsTypes { get; set; }

        [DataMember]
        internal OperatorInputs[] TransformsInputs { get; set; }


        [DataMember]
        internal IMoveDescriptor SecondaryShuffleDescriptor{ get; set; }


        [DataMember]
        internal IDeployDescriptor _deployDescriptor;

        [DataMember]
        internal OperatorEndpointsDescriptor _endpointsDescriptor;

        [DataMember]
        internal ConcurrentDictionary<string, List<ConnectionInfoWithLocality>> _connectionsMap;

        public IDeployDescriptor DeployDescriptor
        {
            set
            {
                _deployDescriptor = value;
            }

            get
            {
                if (_deployDescriptor == null)
                    _deployDescriptor = DeploymentUtils.CreateDefaultDeployDescriptor();

                return _deployDescriptor;
            }
        }

        public OperatorEndpointsDescriptor EndpointsDescriptor
        {
            set
            {
                _endpointsDescriptor = value;
            }

            get
            {
                if (_endpointsDescriptor == null)
                    _endpointsDescriptor = new OperatorEndpointsDescriptor();

                return _endpointsDescriptor;
            }
        }

        public ConcurrentDictionary<string, List<ConnectionInfoWithLocality>> VertexesConnectionsMap
        {
            set
            {
                _connectionsMap = value;
            }

            get
            {
                if (_connectionsMap == null)
                    _connectionsMap = new ConcurrentDictionary<string, List<ConnectionInfoWithLocality>>();

                return _connectionsMap;
            }
        }

        public TaskBase(OperatorType operationType)
        {
            _taskId = NextTaskID();
            _operationType = operationType;
            IsRightOperandInput = false;

            OperationTypes = new BinaryOperatorTypes();
            InputIds = new OperatorInputs();
            NextInputIds = new OperatorInputs();
        }

        public TaskBase() : this(OperatorType.None){ }


        internal static int NextTaskID()
        {
            return ++TaskIdSequence;
        }

        internal void PrepareTaskTransformations(OperatorTransforms nodeMergedOperand)
        {
            if (nodeMergedOperand.Transforms.Count > 0)
            {
                Transforms = nodeMergedOperand.Transforms.ToArray();
                TransformsOperations = nodeMergedOperand.TransformsOperations.ToArray();
                TransformsTypes = nodeMergedOperand.TransformsTypes.ToArray();
                TransformsInputs = nodeMergedOperand.TransformsInputs.ToArray();
            }
        }

        internal void ResetTaskTransformations()
        {
            Transforms = null;
            TransformsOperations = null;
            TransformsTypes = null;
            TransformsInputs = null;
        }
    }
}
