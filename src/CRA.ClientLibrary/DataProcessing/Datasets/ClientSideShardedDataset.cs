using CRA.DataProvider;
using System;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

namespace CRA.ClientLibrary.DataProcessing
{
    public class ClientSideShardedDataset<TKey, TPayload, TDataset>
        : ShardedDatasetBase<TKey, TPayload, TDataset>, IDeployable, IDisposable
        where TDataset : IDataset<TKey, TPayload>
    {
        string _shardedDatasetId = null;
        Expression<Func<int, TDataset>> _producer;

        private CRAClientLibrary _craClient = null;
        private readonly IDataProvider _dataProvider;

        public ClientSideShardedDataset(
            IDataProvider dataProvider,
            Expression<Func<int, TDataset>> producer)
            : base(dataProvider)
        {
            if (producer != null)
                _producer = new ClosureEliminator().Visit(producer) as Expression<Func<int, TDataset>>;
            else
                Console.WriteLine("The producer expression of the ShardedDataset should be provided !!");
            _dataProvider = dataProvider;

            _craClient = new CRAClientLibrary(_dataProvider);
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

        public override async Task<IShardedDataset<TKey, TPayload, TDataset>> Deploy()
        {
            if (!_isDeployed)
            {
                OperatorsToplogy topology = OperatorsToplogy.GetInstance();

                TaskBase subscribeTask = new SubscribeTask();
                subscribeTask.OperationTypes = TransformUtils.FillBinaryTransformTypes(
                                typeof(TKey), typeof(TPayload), typeof(TDataset),
                                typeof(TKey), typeof(TPayload), typeof(TDataset),
                                typeof(TKey), typeof(TPayload), typeof(TDataset));
                subscribeTask.IsRightOperandInput = false;
                OperatorTransforms subscribeInputTransforms = new OperatorTransforms();
                Deploy(ref subscribeTask, ref topology, ref subscribeInputTransforms);
                subscribeTask.InputIds.SetInputId1(subscribeTask.NextInputIds.InputId1);
                subscribeTask.InputIds.SetInputId2(subscribeTask.NextInputIds.InputId2);
                subscribeTask.OutputId = typeof(ShardedSubscribeOperator).Name.ToLower() + Guid.NewGuid().ToString();
                subscribeTask.PrepareTaskTransformations(subscribeInputTransforms);

                topology.AddOperatorBase(subscribeTask.OutputId, subscribeTask);
                topology.AddOperatorInput(subscribeTask.OutputId, subscribeTask.InputIds.InputId1);
                topology.AddOperatorSecondaryInput(subscribeTask.OutputId, subscribeTask.InputIds.InputId2);
                topology.AddOperatorOutput(subscribeTask.InputIds.InputId1, subscribeTask.OutputId);
                topology.AddOperatorOutput(subscribeTask.InputIds.InputId2, subscribeTask.OutputId);

                if (subscribeTask.Transforms != null)
                {
                    foreach (OperatorInputs inputs in subscribeTask.TransformsInputs)
                    {
                        topology.AddOperatorSecondaryInput(subscribeTask.OutputId, inputs.InputId2);
                        topology.AddOperatorOutput(inputs.InputId2, subscribeTask.OutputId);
                    }

                    foreach (OperatorInputs inputs in subscribeTask.TransformsInputs)
                    {
                        if (!topology.ContainsSecondaryOperatorInput(subscribeTask.OutputId, inputs.InputId1))
                        {
                            topology.AddOperatorInput(subscribeTask.OutputId, inputs.InputId1);
                            topology.AddOperatorOutput(inputs.InputId1, subscribeTask.OutputId);
                        }
                    }
                }

                _clientTerminalTask = new ClientTerminalTask();
                _clientTerminalTask.InputIds.SetInputId1(subscribeTask.OutputId);
                _clientTerminalTask.OutputId = typeof(ShardedSubscribeClientOperator).Name.ToLower() + Guid.NewGuid().ToString();
                _clientTerminalTask.OperationTypes = TransformUtils.FillBinaryTransformTypes(
                                typeof(TKey), typeof(TPayload), typeof(TDataset),
                                typeof(TKey), typeof(TPayload), typeof(TDataset),
                                typeof(TKey), typeof(TPayload), typeof(TDataset));

                topology.AddOperatorBase(_clientTerminalTask.OutputId, _clientTerminalTask);
                topology.AddOperatorInput(_clientTerminalTask.OutputId, _clientTerminalTask.InputIds.InputId1);
                topology.AddOperatorInput(_clientTerminalTask.OutputId, _clientTerminalTask.InputIds.InputId2);
                topology.AddOperatorOutput(_clientTerminalTask.InputIds.InputId1, _clientTerminalTask.OutputId);
                topology.AddOperatorOutput(_clientTerminalTask.InputIds.InputId2, _clientTerminalTask.OutputId);

                _isDeployed = await DeploymentUtils.DeployOperators(_craClient, topology);
                if (_isDeployed)
                {
                    string craWorkerName = typeof(ShardedSubscribeClientOperator).Name.ToLower() + "worker" + Guid.NewGuid().ToString();
                    _craWorker = new CRAWorker(craWorkerName, "127.0.0.1", NetworkUtils.GetAvailablePort(), _craClient.DataProvider, null, 1000);
                    _craWorker.DisableDynamicLoading();
                    _craWorker.SideloadVertex(new ShardedSubscribeClientOperator(), typeof(ShardedSubscribeClientOperator).Name.ToLower());
                    new Thread(() => _craWorker.Start()).Start();
                    Thread.Sleep(1000);

                    _isDeployed = await DeploymentUtils.DeployClientTerminal(_craClient, craWorkerName, _clientTerminalTask, topology);
                }
                else
                    return null;
            }

            return this;
        }

        private void GenerateProduceTask(ref OperatorsToplogy operatorsTopology)
        {
            _shardedDatasetId = typeof(ShardedProducerOperator).Name.ToLower() + Guid.NewGuid().ToString();

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

        public async Task ResetCRAClientAsync()
        {
            await _craClient.ResetAsync();
        }
    }
}
