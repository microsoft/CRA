using CRA.DataProvider;
using System;
using System.IO;
using System.Linq.Expressions;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CRA.ClientLibrary.DataProcessing
{
    public class DeployableShardedDataset<TKeyI1, TPayloadI1, TDataSetI1, TKeyI2, TPayloadI2, TDataSetI2, TKeyO, TPayloadO, TDataSetO>
        : ShardedDatasetBase<TKeyO, TPayloadO, TDataSetO>, IDeployable
        where TDataSetI1 : IDataset<TKeyI1, TPayloadI1>
        where TDataSetI2 : IDataset<TKeyI2, TPayloadI2>
        where TDataSetO : IDataset<TKeyO, TPayloadO>
    {
        private readonly IShardedDataset<TKeyI1, TPayloadI1, TDataSetI1> _input1;
        private readonly IShardedDataset<TKeyI2, TPayloadI2, TDataSetI2> _input2;

        private readonly OperatorType _operationType;
        private readonly Expression<Func<TDataSetI1, TDataSetO>> _unaryTransformer;
        private readonly Expression<Func<TDataSetI1, TDataSetI2, TDataSetO>> _binaryTransformer;
        private readonly Expression<Func<TDataSetI1, IMoveDescriptor, TDataSetI2[]>> _splitter;
        private readonly Expression<Func<TDataSetI2[], IMoveDescriptor, TDataSetO>> _merger;
        private readonly IMoveDescriptor _moveDescriptor;

        private CRAClientLibrary _craClient = null;
        private readonly IDataProvider _dataProvider;

        public OperatorType OperationType { get { return _operationType; } }

        internal DeployableShardedDataset(
            IDataProvider dataProvider,
            IShardedDataset<TKeyI1, TPayloadI1, TDataSetI1> input, 
            Expression<Func<TDataSetI1, TDataSetO>> transform)
            : base(dataProvider)
        {
            _input1 = input;
            _input2 = null;
            _operationType = OperatorType.UnaryTransform;

            var closureEliminator = new ClosureEliminator();
            _unaryTransformer = closureEliminator.Visit(transform) as Expression<Func<TDataSetI1, TDataSetO>>;
            _binaryTransformer = null;
            _splitter = null;
            _merger = null;
            _moveDescriptor = null;
            _dataProvider = dataProvider;
        }

        internal DeployableShardedDataset(
            IDataProvider dataProvider,
            IShardedDataset<TKeyI1, TPayloadI1, TDataSetI1> input1,
            IShardedDataset<TKeyI2, TPayloadI2, TDataSetI2> input2,
            Expression<Func<TDataSetI1, TDataSetI2, TDataSetO>> transform)
            : base(dataProvider)
        {
            _input1 = input1;
            _input2 = input2;
            _operationType = OperatorType.BinaryTransform;

            var closureEliminator = new ClosureEliminator();
            _binaryTransformer = closureEliminator.Visit(transform) as Expression<Func<TDataSetI1, TDataSetI2, TDataSetO>>;
            _unaryTransformer = null;
            _splitter = null;
            _merger = null;
            _moveDescriptor = null;
            _dataProvider = dataProvider;
        }

        internal DeployableShardedDataset(
            IDataProvider dataProvider,
            IShardedDataset<TKeyI1, TPayloadI1, TDataSetI1> input,
            Expression<Func<TDataSetI1, IMoveDescriptor, TDataSetI2[]>> splitter,
            Expression<Func<TDataSetI2[], IMoveDescriptor, TDataSetO>> merger, IMoveDescriptor moveDescriptor)
            : base(dataProvider)
        {
            _input1 = input;
            _input2 = null;
            _operationType = OperatorType.Move;

            var closureEliminator = new ClosureEliminator();
            _splitter = closureEliminator.Visit(splitter) as Expression<Func<TDataSetI1, IMoveDescriptor, TDataSetI2[]>>;
            _merger = closureEliminator.Visit(merger) as Expression<Func<TDataSetI2[], IMoveDescriptor, TDataSetO>>;
            _moveDescriptor = moveDescriptor;
            _unaryTransformer = null;
            _binaryTransformer = null;
            _dataProvider = dataProvider;
        }

        public void Deploy(ref TaskBase task, ref OperatorsToplogy topology, ref OperatorTransforms parentTransforms)
        {
            switch (OperationType)
            {
                case OperatorType.Move:
                    DeployMove(ref task, ref topology);
                    break;
                case OperatorType.BinaryTransform:
                    DeployBinaryTransform(ref task, ref topology, ref parentTransforms);
                    break;
                default:
                    DeployUnaryTransform(ref task, ref topology, ref parentTransforms);
                    break;
            }
        }

        private void DeployMove(ref TaskBase task, ref OperatorsToplogy topology)
        {
            var isRightOperandInput = task.IsRightOperandInput;
            OperatorInputs temporaryInputs = new OperatorInputs();

            TaskBase shuffleTask = new ShuffleTask(_moveDescriptor);
            shuffleTask.OperationTypes = TransformUtils.FillBinaryTransformTypes(
                    typeof(TKeyI1), typeof(TPayloadI1), typeof(TDataSetI1),
                    typeof(TKeyI2), typeof(TPayloadI2), typeof(TDataSetI2),
                    typeof(TKeyO), typeof(TPayloadO), typeof(TDataSetO));
            shuffleTask.IsRightOperandInput = false;
            OperatorTransforms shuffleInputTransforms = new OperatorTransforms();
            (_input1 as IDeployable).Deploy(ref shuffleTask, ref topology, ref shuffleInputTransforms);
            shuffleTask.PrepareTaskTransformations(shuffleInputTransforms);
            (shuffleTask as ShuffleTask).MapperVertexName = "shufflemapper" + Guid.NewGuid().ToString();
            (shuffleTask as ShuffleTask).ReducerVertexName = typeof(ShardedShuffleOperator).Name.ToLower() + Guid.NewGuid().ToString();
            shuffleTask.InputIds.SetInputId1(shuffleTask.NextInputIds.InputId1);
            shuffleTask.InputIds.SetInputId2(shuffleTask.NextInputIds.InputId2);
            shuffleTask.OutputId = (shuffleTask as ShuffleTask).ReducerVertexName;
            OperatorTransforms shuffleTransforms = new OperatorTransforms();
            shuffleTransforms.AddTransform(SerializationHelper.Serialize(_splitter),
                    OperatorType.MoveSplit.ToString(),
                    TransformUtils.FillBinaryTransformTypes(typeof(TKeyI1), typeof(TPayloadI1), typeof(TDataSetI1),
                        typeof(TKeyI2), typeof(TPayloadI2), typeof(TDataSetI2),
                        typeof(TKeyO), typeof(TPayloadO), typeof(TDataSetO)).ToString(),
                    shuffleTask.InputIds);
            shuffleTransforms.AddTransform(SerializationHelper.Serialize(_merger),
                    OperatorType.MoveMerge.ToString(),
                    TransformUtils.FillBinaryTransformTypes(typeof(TKeyI1), typeof(TPayloadI1), typeof(TDataSetI1),
                        typeof(TKeyI2), typeof(TPayloadI2), typeof(TDataSetI2),
                        typeof(TKeyO), typeof(TPayloadO), typeof(TDataSetO)).ToString(),
                    shuffleTask.InputIds);
            ((ShuffleTask)shuffleTask).PrepareShuffleTransformations(shuffleTransforms);

            topology.AddShuffleOperator((shuffleTask as ShuffleTask).MapperVertexName, (shuffleTask as ShuffleTask).ReducerVertexName, shuffleTask as ShuffleTask);
            topology.AddOperatorInput((shuffleTask as ShuffleTask).MapperVertexName, shuffleTask.InputIds.InputId1);
            topology.AddOperatorSecondaryInput((shuffleTask as ShuffleTask).MapperVertexName, shuffleTask.InputIds.InputId2);
            topology.AddOperatorOutput(shuffleTask.InputIds.InputId1, (shuffleTask as ShuffleTask).MapperVertexName);
            topology.AddOperatorOutput(shuffleTask.InputIds.InputId2, (shuffleTask as ShuffleTask).MapperVertexName);

            if (shuffleTask.Transforms != null)
            {
                foreach (OperatorInputs inputs in shuffleTask.TransformsInputs)
                {
                    topology.AddOperatorSecondaryInput((shuffleTask as ShuffleTask).MapperVertexName, inputs.InputId2);
                    topology.AddOperatorOutput(inputs.InputId2, (shuffleTask as ShuffleTask).MapperVertexName);
                }

                foreach (OperatorInputs inputs in shuffleTask.TransformsInputs)
                {
                    if (!topology.ContainsSecondaryOperatorInput((shuffleTask as ShuffleTask).MapperVertexName, inputs.InputId1))
                    {
                        topology.AddOperatorInput((shuffleTask as ShuffleTask).MapperVertexName, inputs.InputId1);
                        topology.AddOperatorOutput(inputs.InputId1, (shuffleTask as ShuffleTask).MapperVertexName);
                    }
                }
            }

            // Update the inputs and types for the next operation
            task.InputIds.SetInputId1(shuffleTask.OutputId);
            task.OperationTypes.SetInputKeyType(typeof(TKeyO));
            task.OperationTypes.SetInputPayloadType(typeof(TPayloadO));
            task.OperationTypes.SetInputDatasetType(typeof(TDataSetO));
            if (isRightOperandInput)
                temporaryInputs.InputId2 = shuffleTask.OutputId;
            else
                temporaryInputs.InputId1 = shuffleTask.OutputId;
            task.NextInputIds = temporaryInputs;
        }

        private void DeployBinaryTransform(ref TaskBase task, ref OperatorsToplogy topology, ref OperatorTransforms parentTransforms)
        {
            var isRightOperandInput = task.IsRightOperandInput;
            OperatorInputs temporaryInputs = new OperatorInputs();

            // Prepare the transformations of the operation left operand
            task.IsRightOperandInput = false;
            OperatorTransforms leftTransforms = new OperatorTransforms();
            (_input1 as IDeployable).Deploy(ref task, ref topology, ref leftTransforms);
            temporaryInputs.InputId1 = task.NextInputIds.InputId1;

            // Prepare the transformations of the operation right operand
            task.IsRightOperandInput = true;
            OperatorTransforms rightTransforms = new OperatorTransforms();
            (_input2 as IDeployable).Deploy(ref task, ref topology, ref rightTransforms);
            temporaryInputs.InputId2 = task.NextInputIds.InputId2;

            // Update the inputs for the next operation
            if (isRightOperandInput)
                task.NextInputIds.SetInputId2(temporaryInputs.InputId1);
            else
                task.NextInputIds.SetInputId1(temporaryInputs.InputId1);

            // Merge the tranformations from the two operands, and assign them to the parent operation
            parentTransforms = TransformUtils.MergeTwoSetsOfTransforms(leftTransforms, rightTransforms);
            parentTransforms.AddTransform(SerializationHelper.Serialize(_binaryTransformer),
                OperatorType.BinaryTransform.ToString(),
                TransformUtils.FillBinaryTransformTypes(typeof(TKeyI1), typeof(TPayloadI1), typeof(TDataSetI1),
                    typeof(TKeyI2), typeof(TPayloadI2), typeof(TDataSetI2),
                    typeof(TKeyO), typeof(TPayloadO), typeof(TDataSetO)).ToString(), temporaryInputs);
        }

        private void DeployUnaryTransform(ref TaskBase task, ref OperatorsToplogy topology, ref OperatorTransforms parentTransforms)
        {
            var isRightOperandInput = task.IsRightOperandInput;
            OperatorInputs temporaryInputs = new OperatorInputs();

            // Prepare the transformation of the operation unary (left) operand
            task.IsRightOperandInput = false;
            OperatorTransforms unaryTransforms = new OperatorTransforms();
            (_input1 as IDeployable).Deploy(ref task, ref topology, ref unaryTransforms);
            temporaryInputs.InputId1 = task.NextInputIds.InputId1;

            // Update the inputs for the next operation
            if (isRightOperandInput)
                task.NextInputIds.SetInputId2(temporaryInputs.InputId1);
            else
                task.NextInputIds.SetInputId1(temporaryInputs.InputId1);

            // Add the transformations from this operand to the parent operand
            unaryTransforms.AddTransform(
                SerializationHelper.Serialize(_unaryTransformer),
                OperatorType.UnaryTransform.ToString(),
                TransformUtils.FillUnaryTransformTypes(
                    typeof(TKeyI1),
                    typeof(TPayloadI1),
                    typeof(TDataSetI1),
                    typeof(TKeyO),
                    typeof(TPayloadO),
                    typeof(TDataSetO)).ToString(),
                temporaryInputs);
            parentTransforms = unaryTransforms;
        }

        public override async Task<IShardedDataset<TKeyO, TPayloadO, TDataSetO>> Deploy()
        {
            if (!_isDeployed)
            {
                OperatorsToplogy toplogy = OperatorsToplogy.GetInstance();

                TaskBase subscribeTask = new SubscribeTask();
                subscribeTask.OperationTypes = TransformUtils.FillBinaryTransformTypes(
                                typeof(TKeyO), typeof(TPayloadO), typeof(TDataSetO),
                                typeof(TKeyO), typeof(TPayloadO), typeof(TDataSetO),
                                typeof(TKeyO), typeof(TPayloadO), typeof(TDataSetO));
                subscribeTask.IsRightOperandInput = false;
                OperatorTransforms subscribeInputTransforms = new OperatorTransforms();
                Deploy(ref subscribeTask, ref toplogy, ref subscribeInputTransforms);
                subscribeTask.InputIds.SetInputId1(subscribeTask.NextInputIds.InputId1);
                subscribeTask.InputIds.SetInputId2(subscribeTask.NextInputIds.InputId2);
                subscribeTask.OutputId = typeof(ShardedSubscribeOperator).Name.ToLower() + Guid.NewGuid().ToString();
                subscribeTask.PrepareTaskTransformations(subscribeInputTransforms);

                toplogy.AddOperatorBase(subscribeTask.OutputId, subscribeTask);
                toplogy.AddOperatorInput(subscribeTask.OutputId, subscribeTask.InputIds.InputId1);
                toplogy.AddOperatorSecondaryInput(subscribeTask.OutputId, subscribeTask.InputIds.InputId2);
                toplogy.AddOperatorOutput(subscribeTask.InputIds.InputId1, subscribeTask.OutputId);
                toplogy.AddOperatorOutput(subscribeTask.InputIds.InputId2, subscribeTask.OutputId);

                if (subscribeTask.Transforms != null)
                {
                    foreach (OperatorInputs inputs in subscribeTask.TransformsInputs)
                    {
                        toplogy.AddOperatorSecondaryInput(subscribeTask.OutputId, inputs.InputId2);
                        toplogy.AddOperatorOutput(inputs.InputId2, subscribeTask.OutputId);
                    }

                    foreach (OperatorInputs inputs in subscribeTask.TransformsInputs)
                    {
                        if (!toplogy.ContainsSecondaryOperatorInput(subscribeTask.OutputId, inputs.InputId1))
                        {
                            toplogy.AddOperatorInput(subscribeTask.OutputId, inputs.InputId1);
                            toplogy.AddOperatorOutput(inputs.InputId1, subscribeTask.OutputId);
                        }
                    }
                }

                _clientTerminalTask = new ClientTerminalTask();
                _clientTerminalTask.InputIds.SetInputId1(subscribeTask.OutputId);
                _clientTerminalTask.OutputId = typeof(ShardedSubscribeClientOperator).Name.ToLower() + Guid.NewGuid().ToString();
                _clientTerminalTask.OperationTypes = TransformUtils.FillBinaryTransformTypes(
                                typeof(TKeyO), typeof(TPayloadO), typeof(TDataSetO),
                                typeof(TKeyO), typeof(TPayloadO), typeof(TDataSetO),
                                typeof(TKeyO), typeof(TPayloadO), typeof(TDataSetO));

                _craClient = new CRAClientLibrary(_dataProvider);
                toplogy.AddOperatorBase(_clientTerminalTask.OutputId, _clientTerminalTask);
                toplogy.AddOperatorInput(_clientTerminalTask.OutputId, _clientTerminalTask.InputIds.InputId1);
                toplogy.AddOperatorInput(_clientTerminalTask.OutputId, _clientTerminalTask.InputIds.InputId2);
                toplogy.AddOperatorOutput(_clientTerminalTask.InputIds.InputId1, _clientTerminalTask.OutputId);
                toplogy.AddOperatorOutput(_clientTerminalTask.InputIds.InputId2, _clientTerminalTask.OutputId);

                _isDeployed = await DeploymentUtils.DeployOperators(_craClient, toplogy);
                if (_isDeployed)
                {
                    string craWorkerName = typeof(ShardedSubscribeClientOperator).Name.ToLower() + "worker" + Guid.NewGuid().ToString();
                    _craWorker = new CRAWorker(craWorkerName, "127.0.0.1", NetworkUtils.GetAvailablePort(), _craClient.DataProvider, null, 1000);
                    _craWorker.DisableDynamicLoading();
                    _craWorker.SideloadVertex(new ShardedSubscribeClientOperator(), typeof(ShardedSubscribeClientOperator).Name.ToLower());
                    new Thread(() => _craWorker.Start()).Start();
                    Thread.Sleep(1000);

                    _isDeployed = await DeploymentUtils.DeployClientTerminal(_craClient, craWorkerName, _clientTerminalTask, toplogy);
                }
                else
                    return null;
            }

            return this;
        }
    }
}
