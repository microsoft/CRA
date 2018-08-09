using System;
using System.Linq.Expressions;

namespace CRA.ClientLibrary.DataProcessing
{
    public class ClientSideShardedDataset<TKey, TPayload, TDataset> : ShardedDatasetBase<TKey, TPayload, TDataset>, IDeployable, IDisposable
        where TDataset : IDataset<TKey, TPayload>
    {
        string _shardedDatasetId;
        Expression<Func<int, TDataset>> _producer;

        bool _isDeployed = false;
        private CRAClientLibrary _craClient = null;

        public ClientSideShardedDataset(Expression<Func<int, TDataset>> producer)
        {
            if (producer != null)
                _producer = new ClosureEliminator().Visit(producer) as Expression<Func<int, TDataset>>;
            else
                Console.WriteLine("The producer expression of the ShardedDataset should be provided !!");
        }

        public void Deploy(ref TaskBase task, ref OperatorsToplogy operatorsTopology, ref OperatorTransforms operandTransforms)
        {
            GenerateProduceTask(ref operatorsTopology);

            var isRightOperandInput = task.IsRightOperandInput;
            if (isRightOperandInput)
            {
                task.InputIds.SetInputId2(_shardedDatasetId);
                task.NextInputIds.SetInputId2(_shardedDatasetId);
                task.OperationTypes.SetSecondaryKeyType(typeof(TKey));
                task.OperationTypes.SetSecondaryPayloadType(typeof(TPayload));
                task.OperationTypes.SetSecondaryDatasetType(typeof(TDataset));
            }
            else
            {
                task.InputIds.SetInputId1(_shardedDatasetId);
                task.NextInputIds.SetInputId1(_shardedDatasetId);
                task.OperationTypes.SetInputKeyType(typeof(TKey));
                task.OperationTypes.SetInputPayloadType(typeof(TPayload));
                task.OperationTypes.SetInputDatasetType(typeof(TDataset));
            }

            task.IsRightOperandInput = false;
        }

        public override IShardedDataset<TKey, TPayload, TDataset> Deploy()
        {
            if (!_isDeployed)
            {
                OperatorsToplogy operatorsTopology = OperatorsToplogy.GetInstance();

                GenerateProduceTask(ref operatorsTopology);

                _craClient = new CRAClientLibrary();
                _isDeployed =  DeploymentUtils.DeployOperators(_craClient, operatorsTopology);
                if (!_isDeployed) return null;
            }

            return this;
        }

        private void GenerateProduceTask(ref OperatorsToplogy operatorsTopology)
        {
            _shardedDatasetId = typeof(ProducerOperator).Name.ToLower() + Guid.NewGuid().ToString();

            TaskBase produceTask = new ProduceTask(SerializationHelper.Serialize(_producer));
            produceTask.OperationTypes = TransformUtils.FillBinaryTransformTypes(
                            typeof(TKey), typeof(TPayload), typeof(TDataset),
                            typeof(TKey), typeof(TPayload), typeof(TDataset),
                            typeof(TKey), typeof(TPayload), typeof(TDataset));
            produceTask.IsRightOperandInput = false;
            produceTask.InputIds.SetInputId1(_shardedDatasetId);
            produceTask.InputIds.SetInputId2(_shardedDatasetId);
            produceTask.OutputId = _shardedDatasetId;
            produceTask.NextInputIds.SetInputId1(_shardedDatasetId);
            produceTask.NextInputIds.SetInputId2(_shardedDatasetId);
            produceTask.PrepareTaskTransformations(new OperatorTransforms());

            operatorsTopology.AddOperatorBase(produceTask.OutputId, produceTask);
        }

        public override void Subscribe<TDatasetObserver>(Expression<Func<TDatasetObserver>> observer)
        {
            if (!_isDeployed) Deploy();
            //TODO: to be implemented here
        }

        public override void MultiSubscribe<TDatasetObserver>(Expression<Func<TDatasetObserver>> observer, int runsCount)
        {
            if (!_isDeployed) Deploy();
            //TODO: to be implemented here
        }


        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool isDisposing)
        {
            if (isDisposing)
            {
                _craClient.Dispose();
            }
        }

        public override void Consume<TDatasetConsumer>(Expression<Func<TDatasetConsumer>> consumer)
        {
            if (!_isDeployed) Deploy();
            throw new NotImplementedException();
        }
    }
}
