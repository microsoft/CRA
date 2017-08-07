namespace CRA.ClientLibrary.DataProcessing
{
    internal interface IDeployable
    {
        void Deploy(ref TaskBase task, ref OperatorsToplogy topology, ref OperatorTransforms operandTransforms);
    }
}
