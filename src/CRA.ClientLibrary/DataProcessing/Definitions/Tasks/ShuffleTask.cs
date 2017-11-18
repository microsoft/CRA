using System.Runtime.Serialization;

namespace CRA.ClientLibrary.DataProcessing
{
    [DataContract]
    public class ShuffleTask : TaskBase
    {
        [DataMember]
        internal string MapperVertexName { get; set; }

        [DataMember]
        internal string ReducerVertexName { get; set; }

        [DataMember]
        internal string[] ShuffleTransforms { get; set; }

        [DataMember]
        internal string[] ShuffleTransformsOperations { get; set; }

        [DataMember]
        internal string[] ShuffleTransformsTypes { get; set; }

        [DataMember]
        internal OperatorInputs[] ShuffleTransformsInputs { get; set; }

        [DataMember]
        internal IMoveDescriptor ShuffleDescriptor { get; set; }

        [DataMember]
        internal OperatorEndpointsDescriptor _secondaryEndpointsDescriptor;

        public OperatorEndpointsDescriptor SecondaryEndpointsDescriptor
        {
            set
            {
                _secondaryEndpointsDescriptor = value;
            }

            get
            {
                if (_secondaryEndpointsDescriptor == null)
                    _secondaryEndpointsDescriptor = new OperatorEndpointsDescriptor();

                return _secondaryEndpointsDescriptor;
            }
        }

        public ShuffleTask(IMoveDescriptor shuffleDescriptor) : base(OperatorType.Move)
        {
            ShuffleDescriptor = shuffleDescriptor;
        }

        internal void PrepareShuffleTransformations(OperatorTransforms nodeMergedOperand)
        {
            if (nodeMergedOperand.Transforms.Count > 0)
            {
                ShuffleTransforms = nodeMergedOperand.Transforms.ToArray();
                ShuffleTransformsOperations = nodeMergedOperand.TransformsOperations.ToArray();
                ShuffleTransformsTypes = nodeMergedOperand.TransformsTypes.ToArray();
                ShuffleTransformsInputs = nodeMergedOperand.TransformsInputs.ToArray();
            }
        }

    }
}
