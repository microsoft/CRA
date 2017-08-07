using System.Collections.Generic;

namespace CRA.ClientLibrary.DataProcessing
{
    public class OperatorTransforms
    {
        public List<string> Transforms { get; set; }
        public List<string> TransformsOperations { get; set; }
        public List<string> TransformsTypes { get; set; }
        public List<OperatorInputs> TransformsInputs { get; set; }

        public OperatorTransforms()
        {
            Transforms = new List<string>();
            TransformsOperations = new List<string>();
            TransformsTypes = new List<string>();
            TransformsInputs = new List<OperatorInputs>();
        }

        public OperatorTransforms(List<string> transforms, List<string> transformsOperations,
                                  List<string> transformsTypes, List<OperatorInputs> tranformsInputs)
        {
            Transforms = transforms;
            TransformsOperations = transformsOperations;
            TransformsTypes = transformsTypes;
            TransformsInputs = tranformsInputs;
        }

        public void AddTransform(string transform, string transformOperation, string transformType, OperatorInputs transformInput)
        {
            Transforms.Add(transform);
            TransformsOperations.Add(transformOperation);
            TransformsTypes.Add(transformType);
            TransformsInputs.Add(transformInput);
        }
    }
}
