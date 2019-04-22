using CRA.ClientLibrary.DataProvider;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace CRA.ClientLibrary.DataProcessing
{
    public class ShuffleOperator : OperatorBase
    {
        private IEndpointContent[] _secondaryInputs;
        private string[] _secondaryInputsIds;
        private ConcurrentDictionary<int, string> _secondaryInputEndpointOperatorIndex;
        private ConcurrentDictionary<string, List<Tuple<int, bool>>> _secondaryInputEndpointConnectStatus;
        private ConcurrentDictionary<string, List<Tuple<int, bool>>> _secondaryInputEndpointTriggerStatus;
        private ConcurrentDictionary<IEndpointContent, int> _secondaryInputEndpointInvertedIndex;
        private ConcurrentDictionary<string, ManualResetEvent> _onCompletedSecondaryInputs;

        internal Dictionary<string, object> _cachedDatasets; 

        private Type _outputKeyType;
        private Type _outputPayloadType;
        private Type _outputDatasetType;
        private string _outputId;

        private System.Object _applySecondaryInputsLock = new System.Object();
        private bool _isSecondaryInputsApplied = false;

        private System.Object _produceIfReadyLock = new System.Object();
        private bool _isProduceIfReadyApplied = false;

        private ManualResetEvent _firstProduceTrigger;
        private bool _isProduceAllowedToApply = true;


        public ShuffleOperator(IDataProvider dataProvider) : base(dataProvider)
        {
            _cachedDatasets = new Dictionary<string, object>();
            _firstProduceTrigger = new ManualResetEvent(false);
        }

        internal override void InitializeOperator()
        {
            IVertex thisOperator = this;
            if (_inputsIds != null)
                for (int i = 0; i < _inputsIds.Length; i++)
                {
                    var fromTuple = _toFromConnections[new Tuple<string, string>(VertexName, _inputsIds[i])];
                    if (fromTuple.Item3)
                        AddAsyncInputEndpoint(_inputsIds[i], new OperatorFusableInput(ref thisOperator, i));
                    else
                        AddAsyncInputEndpoint(_inputsIds[i], new OperatorInput(ref thisOperator, i));
                }

            if (_secondaryInputsIds != null)
                for (int i = 0; i < _secondaryInputsIds.Length; i++)
                {
                    var fromTuple = _toFromConnections[new Tuple<string, string>(VertexName, _secondaryInputsIds[i])];
                    if (fromTuple.Item3)
                        AddAsyncInputEndpoint(_secondaryInputsIds[i], new OperatorFusableInput(ref thisOperator, i, true));
                    else
                        AddAsyncInputEndpoint(_secondaryInputsIds[i], new OperatorInput(ref thisOperator, i, true));
                }

            if (_outputsIds != null)
                for (int i = 0; i < _outputsIds.Length; i++)
                {
                    var toTuple = _fromToConnections[new Tuple<string, string>(VertexName, _outputsIds[i])];
                    if (toTuple.Item3)
                        AddAsyncOutputEndpoint(_outputsIds[i], new OperatorFusableOutput(ref thisOperator, i));
                    else
                        AddAsyncOutputEndpoint(_outputsIds[i], new OperatorOutput(ref thisOperator, i));
                }
        }

        internal override void AddSecondaryInput(int i, ref IEndpointContent endpoint)
        {
            if (_secondaryInputs != null && _secondaryInputEndpointConnectStatus != null && _secondaryInputEndpointOperatorIndex != null && _secondaryInputEndpointInvertedIndex != null)
            {
                _secondaryInputs[i] = endpoint;
                _secondaryInputEndpointInvertedIndex.AddOrUpdate(_secondaryInputs[i], i, (key, value) => i);
                UpdateEndpointStatus(_secondaryInputEndpointConnectStatus, _secondaryInputEndpointOperatorIndex, i, true);
                if (AreAllEndpointsReady(_secondaryInputEndpointConnectStatus, true))
                {
                    lock (_applySecondaryInputsLock)
                    {
                        if (!_isSecondaryInputsApplied)
                        {
                            int[] endpointsIndices = new int[_secondaryInputs.Length];
                            for (int j = 0; j < _secondaryInputs.Length; j++)
                                endpointsIndices[j] = j;

                            ApplyOperatorSecondaryInput(endpointsIndices);
                            _isSecondaryInputsApplied = true;
                        }
                    }
                }
            }
        }

        private void ApplyOperatorSecondaryInput(int[] endpointsIndices)
        {
            for (int i = 0; i < endpointsIndices.Length; i++)
                UpdateEndpointStatus(_secondaryInputEndpointTriggerStatus, _secondaryInputEndpointOperatorIndex, endpointsIndices[i], true);
        }

        internal override void ApplyOperatorInput(int[] inputIndices)
        {
            for (int i = 0; i < inputIndices.Length; i++)
                UpdateEndpointStatus(_inputEndpointTriggerStatus, _inputEndpointOperatorIndex, inputIndices[i], true);

            StartMergeAndTransform();
        }

        private async void StartMergeAndTransform()
        {
            var shuffleTask = (ShuffleTask)_task;

            BinaryOperatorTypes mergeTypes = new BinaryOperatorTypes();
            mergeTypes.FromString(shuffleTask.ShuffleTransformsTypes[1]);
            MethodInfo method = typeof(MoveUtils).GetMethod("ApplyMerger");
            MethodInfo generic = method.MakeGenericMethod(
                new Type[] {mergeTypes.SecondaryKeyType, mergeTypes.SecondaryPayloadType,
                            mergeTypes.SecondaryDatasetType, mergeTypes.OutputKeyType,
                            mergeTypes.OutputPayloadType, mergeTypes.OutputDatasetType
                });
            object[] inputSplitDatasets = new object[_inputs.Length];
            for (int i = 0; i < _inputs.Length; i++)
            {
                int splitIndex = Convert.ToInt32(_inputEndpointOperatorIndex[i].Substring(_inputEndpointOperatorIndex[i].Length - 1));
                inputSplitDatasets[splitIndex] = CreateDatasetFromInput(_inputEndpointOperatorIndex[i], mergeTypes.SecondaryKeyType, mergeTypes.SecondaryPayloadType,
                                                    mergeTypes.SecondaryDatasetType);
            }
            object[] arguments = new Object[] { inputSplitDatasets, shuffleTask.ShuffleDescriptor, shuffleTask.ShuffleTransforms[1] };
            _cachedDatasets[shuffleTask.OutputId] = generic.Invoke(this, arguments);

            _outputKeyType = mergeTypes.OutputKeyType;
            _outputPayloadType = mergeTypes.OutputPayloadType;
            _outputDatasetType = mergeTypes.OutputDatasetType;
            _outputId = shuffleTask.OutputId;

            if (_task.Transforms != null)
            { 
                for (int i = 0; i < _task.Transforms.Length; i++)
                {
                    object dataset1 = null; string dataset1Id = null;
                    object dataset2 = null; string dataset2Id = null;
                    TransformUtils.PrepareTransformInputs(_task.TransformsInputs[i], ref dataset1, ref dataset1Id,
                                        ref dataset2, ref dataset2Id, _cachedDatasets);

                    string transformType = _task.TransformsOperations[i];
                    object transformOutput = null;
                    if (transformType == OperatorType.UnaryTransform.ToString())
                    {
                        UnaryOperatorTypes unaryTransformTypes = new UnaryOperatorTypes();
                        unaryTransformTypes.FromString(_task.TransformsTypes[i]);
                        if (dataset1Id == "$" && dataset1 == null)
                            throw new InvalidOperationException();

                        method = typeof(TransformUtils).GetMethod("ApplyUnaryTransformer");
                        generic = method.MakeGenericMethod(
                                new Type[] { unaryTransformTypes.InputKeyType, unaryTransformTypes.InputPayloadType,
                                        unaryTransformTypes.InputDatasetType, unaryTransformTypes.OutputKeyType,
                                        unaryTransformTypes.OutputPayloadType, unaryTransformTypes.OutputDatasetType
                                });
                        arguments = new Object[] { dataset1, _task.Transforms[i] };

                        _outputKeyType = unaryTransformTypes.OutputKeyType;
                        _outputPayloadType = unaryTransformTypes.OutputPayloadType;
                        _outputDatasetType = unaryTransformTypes.OutputDatasetType;
                    }
                    else if (transformType == OperatorType.BinaryTransform.ToString())
                    {
                        BinaryOperatorTypes binaryTransformTypes = new BinaryOperatorTypes();
                        binaryTransformTypes.FromString(_task.TransformsTypes[i]);
                        if (dataset1Id == "$" && dataset1 == null)
                                    throw new InvalidOperationException();
                        if (dataset2Id == "$" && dataset2 == null)
                        {
                            dataset2Id = _task.TransformsInputs[i].InputId2;
                            dataset2 = CreateDatasetFromSecondaryInput(dataset2Id, binaryTransformTypes.SecondaryKeyType,
                                                                binaryTransformTypes.SecondaryPayloadType, binaryTransformTypes.SecondaryDatasetType);
                            if (!_cachedDatasets.ContainsKey(dataset2Id))
                                _cachedDatasets.Add(dataset2Id, dataset2);
                            else
                                _cachedDatasets[dataset2Id] = dataset2;
                        }

                        method = typeof(TransformUtils).GetMethod("ApplyBinaryTransformer");
                        generic = method.MakeGenericMethod(
                            new Type[] {binaryTransformTypes.InputKeyType, binaryTransformTypes.InputPayloadType,
                                binaryTransformTypes.InputDatasetType, binaryTransformTypes.SecondaryKeyType,
                                binaryTransformTypes.SecondaryPayloadType, binaryTransformTypes.SecondaryDatasetType,
                                binaryTransformTypes.OutputKeyType, binaryTransformTypes.OutputPayloadType,
                                binaryTransformTypes.OutputDatasetType
                            });
                        arguments = new Object[] { dataset1, dataset2, _task.Transforms[i] };

                        _outputKeyType = binaryTransformTypes.OutputKeyType;
                        _outputPayloadType = binaryTransformTypes.OutputPayloadType;
                        _outputDatasetType = binaryTransformTypes.OutputDatasetType;
                    }
                    else if (transformType == OperatorType.MoveSplit.ToString())
                    {
                        BinaryOperatorTypes splitTypes = new BinaryOperatorTypes();
                        splitTypes.FromString(_task.TransformsTypes[i]);
                        if (dataset1Id == "$" && dataset1 == null)
                            throw new InvalidOperationException();

                        method = typeof(MoveUtils).GetMethod("ApplySplitter");
                        generic = method.MakeGenericMethod(
                            new Type[] {splitTypes.InputKeyType, splitTypes.InputPayloadType,
                                    splitTypes.InputDatasetType, splitTypes.SecondaryKeyType,
                                    splitTypes.SecondaryPayloadType, splitTypes.SecondaryDatasetType
                            });
                        arguments = new Object[] { dataset1, _task.SecondaryShuffleDescriptor, _task.Transforms[i] };

                        _outputKeyType = splitTypes.SecondaryKeyType;
                        _outputPayloadType = splitTypes.SecondaryPayloadType;
                        _outputDatasetType = splitTypes.SecondaryDatasetType;
                    }
                    else
                        throw new InvalidOperationException("Error: Unsupported transformation type");

                    if (method != null && generic != null && arguments != null)
                        transformOutput = generic.Invoke(this, arguments);
                    if (transformOutput != null)
                    {
                        if (!_cachedDatasets.ContainsKey(dataset1Id))
                            _cachedDatasets.Add(dataset1Id, transformOutput);
                        else
                            _cachedDatasets[dataset1Id] = transformOutput;
                    }

                    _outputId = dataset1Id;
                }
            }

            await Task.Run(() => ApplyProducer());

            _isProduceAllowedToApply = false;
            _firstProduceTrigger.Set();
        }

        private Task<bool> OnReceivedReadyMessage(int outputId)
        {
            return ((ObjectEndpoint)_outputs[outputId]).OwningOutputEndpoint.InputEndpoint.EndpointContent.OnReceivedReadyMessage();
        }

        private Task<bool> OnReceivedReleaseMessage(int outputId)
        {
            return ((ObjectEndpoint)_outputs[outputId]).OwningOutputEndpoint.InputEndpoint.EndpointContent.OnReceivedReleaseMessage();
        }

        private bool AreAllFlagsTrue(bool[] flags)
        {
            bool areAllFlagsTrue = true;
            for (int i = 0; i < flags.Length; i++)
            {
                if (flags[i] == false)
                {
                    areAllFlagsTrue = false;
                    break;
                }
            }
            return areAllFlagsTrue;
        }

        private async Task<bool> isReleaseAcquired()
        {
            bool[] releaseFlags = new bool[_outputs.Length];
            for (int i = 0; i < releaseFlags.Length; i++)
                releaseFlags[i] = false;

            bool[] reuseFlags = new bool[_outputs.Length];
            for (int i = 0; i < reuseFlags.Length; i++)
                reuseFlags[i] = false;

            for (int i = 0; i < _outputs.Length; i++)
            {
                if (_outputs[i] as StreamEndpoint != null)
                {
                    CRATaskMessageType message = (CRATaskMessageType)(await ((StreamEndpoint)_outputs[i]).Stream.ReadInt32Async());
                    if (message == CRATaskMessageType.READY)
                        reuseFlags[i] = true;
                    else if (message == CRATaskMessageType.RELEASE)
                        releaseFlags[i] = true;
                    else
                        throw new InvalidOperationException();
                }
                else
                {
                    int currentIndex = i;
                    int receivedMessageType = Task.WaitAny(new Task<bool>[] { Task.Run(() => OnReceivedReadyMessage(currentIndex)), Task.Run(() => OnReceivedReleaseMessage(currentIndex)) });
                    if (receivedMessageType == 0)
                        reuseFlags[i] = true;
                    else
                        releaseFlags[i] = true;
                }
            }

            if (AreAllFlagsTrue(releaseFlags))
            {
                foreach (string operatorId in _inputEndpointTriggerStatus.Keys)
                    _onCompletedInputs[operatorId].Set();

                foreach (string operatorId in _secondaryInputEndpointTriggerStatus.Keys)
                    _onCompletedSecondaryInputs[operatorId].Set();

                foreach (string operatorId in _outputEndpointTriggerStatus.Keys)
                    _onCompletedOutputs[operatorId].Set();

                if (_inputs != null)
                {
                    for (int i = 0; i < _inputs.Length; i++)
                    {
                        if (_inputs[i] as StreamEndpoint != null)
                            ((StreamEndpoint)_inputs[i]).Stream.WriteInt32((int)CRATaskMessageType.RELEASE);
                        else
                            ((ObjectEndpoint)_inputs[i]).ReleaseTrigger.Set();
                    }
                }

                if (_secondaryInputs != null)
                {
                    for (int i = 0; i < _secondaryInputs.Length; i++)
                    {
                        if (_secondaryInputs[i] as StreamEndpoint != null)
                            ((StreamEndpoint)_secondaryInputs[i]).Stream.WriteInt32((int)CRATaskMessageType.RELEASE);
                        else
                            ((ObjectEndpoint)_secondaryInputs[i]).ReleaseTrigger.Set();
                    }
                }
                return true;
            }
            else
                return false;
        }
        
        private void ApplyProducer()
        {
            bool isSplitProducer = false;
            if (_task.Transforms != null && _task.Transforms.Length != 0 &&
                    _task.TransformsOperations[_task.Transforms.Length - 1] == OperatorType.MoveSplit.ToString())
                isSplitProducer = true;

            Task<bool>[] tasks = new Task<bool>[_outputsIds.Length];
            for (int i = 0; i < tasks.Length; i++)
            {
                int taskIndex = i;

                if (isSplitProducer)
                    tasks[taskIndex] = StartSplitProducerAsync(taskIndex);
                else
                    tasks[taskIndex] = StartProducerAsync(taskIndex);
            }
            bool[] results = Task.WhenAll(tasks).Result;

            bool isSuccess = true;
            for (int i = 0; i < results.Length; i++)
                if (!results[i])
                {
                    isSuccess = false;
                    break;
                }

            if (!isSuccess)
                throw new InvalidOperationException();
        }

        private Task<bool> StartProducerAsync(int endpointIndex)
        {
            return Task.Factory.StartNew(() => {
                MethodInfo method = typeof(OperatorBase).GetMethod("StartProducer");
                MethodInfo generic = method.MakeGenericMethod(
                        new Type[] { _outputKeyType, _outputPayloadType, _outputDatasetType });
                generic.Invoke(this, new Object[] { new object[]{_cachedDatasets[_outputId]}, GetSiblingEndpointsByEndpointId(_outputEndpointTriggerStatus,
                        _outputEndpointOperatorIndex, endpointIndex), 1 });
                return true;
            });
        }

        private Task<bool> StartSplitProducerAsync(int endpointIndex)
        {
            return Task.Factory.StartNew(() => {
                int splitIndex = Convert.ToInt32(_outputEndpointOperatorIndex[endpointIndex].Substring(_outputEndpointOperatorIndex[endpointIndex].Length - 1));
                object[] splitDatasets = (object[])_cachedDatasets[_outputId];
                MethodInfo method = typeof(OperatorBase).GetMethod("StartProducer");
                MethodInfo generic = method.MakeGenericMethod(
                        new Type[] { _outputKeyType, _outputPayloadType, _outputDatasetType });
                generic.Invoke(this, new Object[] { new object[]{splitDatasets[splitIndex]}, GetSiblingEndpointsByEndpointId(_outputEndpointTriggerStatus,
                        _outputEndpointOperatorIndex, endpointIndex), 1 });
                return true;
            });
        }

        private object CreateDatasetFromSecondaryInput(string operatorId, Type inputKeyType, Type inputPayloadType, Type inputDatasetType)
        {
            try
            {
                int[] endpointsIndices = GetSiblingInputEndpointsByOperatorId(_secondaryInputEndpointTriggerStatus, operatorId);
                MethodInfo method = typeof(OperatorBase).GetMethod("CreateDataset");
                MethodInfo generic = method.MakeGenericMethod(
                                        new Type[] { inputKeyType, inputPayloadType, inputDatasetType });
                return generic.Invoke(this, new Object[] { endpointsIndices });
            }
            catch (Exception e)
            {
                throw new Exception("Error: Failed to create dataset from input!! " + e.ToString());
            }
        }

        internal override void ApplyOperatorOutput(int[] outputIndices)
        {
            for (int i = 0; i < outputIndices.Length; i++)
            {
                int currentIndex = outputIndices[i];
                Task.Run(() => StartProducerAfterTrigger(currentIndex));
            }
        }

        private async void StartProducerAfterTrigger(int outputIndex)
        {
            if (_outputs[outputIndex] as StreamEndpoint != null)
            {
                CRATaskMessageType message = (CRATaskMessageType)(await ((StreamEndpoint)_outputs[outputIndex]).Stream.ReadInt32Async());
                if (message == CRATaskMessageType.READY)
                {
                    StartProducerIfReady(outputIndex);
                }
            }
            else
            {
                bool isReceived = await ((ObjectEndpoint)_outputs[outputIndex]).OwningOutputEndpoint.InputEndpoint.EndpointContent.OnReceivedReadyMessage();
                if (isReceived)
                {
                    ((ObjectEndpoint)_outputs[outputIndex]).OwningOutputEndpoint.InputEndpoint.EndpointContent.ReadyTrigger.Reset();
                    StartProducerIfReady(outputIndex);
                }
            }
        }

        private void StartProducerIfReady(int outputIndex)
        {
            UpdateEndpointStatus(_outputEndpointTriggerStatus, _outputEndpointOperatorIndex, outputIndex, true);
            if (AreAllEndpointsReady(_outputEndpointTriggerStatus, true))
            {
                if (AreAllEndpointsReady(_inputEndpointConnectStatus, true))
                {
                    lock (_produceIfReadyLock)
                    {
                        if (!_isProduceIfReadyApplied)
                        {
                            if (_inputs != null)
                            {
                                for (int i = 0; i < _inputs.Length; i++)
                                {
                                    if (_inputs[i] as StreamEndpoint != null)
                                        ((StreamEndpoint)_inputs[i]).Stream.WriteInt32((int)CRATaskMessageType.READY);
                                    else
                                        ((ObjectEndpoint)_inputs[i]).ReadyTrigger.Set();
                                }
                            }

                            if (_secondaryInputs != null)
                            {
                                for (int i = 0; i < _secondaryInputs.Length; i++)
                                {
                                    if (_secondaryInputs[i] as StreamEndpoint != null)
                                        ((StreamEndpoint)_secondaryInputs[i]).Stream.WriteInt32((int)CRATaskMessageType.READY);
                                    else
                                        ((ObjectEndpoint)_secondaryInputs[i]).ReadyTrigger.Set();
                                }
                            }

                            if (_firstProduceTrigger.WaitOne())
                            {
                                if (_isProduceAllowedToApply) ApplyProducer();

                                _isProduceAllowedToApply = true;
                            }
                
                            if (AreAllEndpointsReady(_inputEndpointTriggerStatus, true) && AreAllEndpointsReady(_secondaryInputEndpointTriggerStatus, true)
                                    && AreAllEndpointsReady(_outputEndpointTriggerStatus, true))
                            {
                                bool[] isReleased = Task.WhenAll(new Task<bool>[] { isReleaseAcquired() }).Result;
                                if (!isReleased[0])
                                {
                                    _isProduceIfReadyApplied = false;
                                    for (int i = 0; i < _outputs.Length; i++)
                                    {
                                        if (_outputs[i] as ObjectEndpoint != null)
                                            ((ObjectEndpoint)_outputs[i]).OwningOutputEndpoint.InputEndpoint.EndpointContent.ReadyTrigger.Reset();
                                    }

                                    StartProducerIfReady(0);
                                }
                            }

                            _isProduceIfReadyApplied = true;
                        }
                    }
                }
            }
        }

        internal override void PrepareOperatorInput()
        {
            base.PrepareOperatorInput();
            PrepareOperatorSecondaryInput();
        }

        private void PrepareOperatorSecondaryInput()
        {
            _secondaryInputsIds = ToEndpointsIds(((ShuffleTask)_task).SecondaryEndpointsDescriptor, false);
            if (_secondaryInputsIds != null && _secondaryInputsIds.Length > 0)
                _secondaryInputs = new IEndpointContent[_secondaryInputsIds.Length];
            _secondaryInputEndpointOperatorIndex = ToEndpointOperatorIndex(((ShuffleTask)_task).SecondaryEndpointsDescriptor.FromInputs);
            _secondaryInputEndpointConnectStatus = ToEndpointStatus(_secondaryInputEndpointOperatorIndex);
            _secondaryInputEndpointTriggerStatus = ToEndpointStatus(_secondaryInputEndpointOperatorIndex);
            _secondaryInputEndpointInvertedIndex = new ConcurrentDictionary<IEndpointContent, int>();
            _onCompletedSecondaryInputs = new ConcurrentDictionary<string, ManualResetEvent>();
            foreach (var operatorId in ((ShuffleTask)_task).SecondaryEndpointsDescriptor.FromInputs.Keys)
                _onCompletedSecondaryInputs.AddOrUpdate(operatorId, new ManualResetEvent(false), (key, value) => new ManualResetEvent(false));
        }

        internal override void PrepareOperatorOutput()
        {
            base.PrepareOperatorOutput();
        }

        internal override void WaitForSecondaryInputCompletion(int i)
        {
            if (_onCompletedSecondaryInputs != null && _secondaryInputEndpointOperatorIndex != null)
                _onCompletedSecondaryInputs[_secondaryInputEndpointOperatorIndex[i]].WaitOne();
        }

        internal override void RemoveSecondaryInput(int i)
        {
            if (_secondaryInputs != null && _secondaryInputEndpointConnectStatus != null && _secondaryInputEndpointOperatorIndex != null)
            {
                UpdateEndpointStatus(_secondaryInputEndpointConnectStatus, _secondaryInputEndpointOperatorIndex, i, false);
                if (AreSiblingEndpointsReady(_secondaryInputEndpointConnectStatus, _secondaryInputEndpointOperatorIndex, i, false))
                    ResetAfterSecondaryInputCompletion(i);
            }
        }

        private void ResetAfterSecondaryInputCompletion(int i)
        {
            if (_onCompletedSecondaryInputs != null && _secondaryInputEndpointOperatorIndex != null)
                _onCompletedSecondaryInputs[_secondaryInputEndpointOperatorIndex[i]].Reset();
        }

    }
}
