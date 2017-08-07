using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace CRA.ClientLibrary.DataProcessing
{
    public class ShuffleOperator : OperatorBase
    {
        private Stream[] _secondaryInputs;
        private string[] _secondaryInputsIds;
        private ConcurrentDictionary<int, string> _secondaryInputStreamOperatorIndex;
        private ConcurrentDictionary<string, List<Tuple<int, bool>>> _secondaryInputStreamConnectStatus;
        private ConcurrentDictionary<string, List<Tuple<int, bool>>> _secondaryInputStreamTriggerStatus;
        private ConcurrentDictionary<Stream, int> _secondaryInputStreamsInvertedIndex;
        private ConcurrentDictionary<string, ManualResetEvent> _onCompletedSecondaryInputs;

        internal Dictionary<string, object> _cachedDatasets;

        private Type _outputKeyType;
        private Type _outputPayloadType;
        private Type _outputDatasetType;
        private string _outputId;

        public ShuffleOperator() : base()
        {
            _cachedDatasets = new Dictionary<string, object>();
        }

        internal override void InitializeOperator()
        {
            if (_inputsIds != null)
            {
                for (int i = 0; i < _inputsIds.Length; i++)
                    AddAsyncInputEndpoint(_inputsIds[i], new OperatorInput(this, i));
            }

            if (_secondaryInputsIds != null)
            {
                for (int i = 0; i < _secondaryInputsIds.Length; i++)
                    AddAsyncInputEndpoint(_secondaryInputsIds[i], new OperatorInput(this, i, true));
            }

            if (_outputsIds != null)
            {
                for (int i = 0; i < _outputsIds.Length; i++)
                    AddAsyncOutputEndpoint(_outputsIds[i], new OperatorOutput(this, i));
            }
        }

        internal override void AddSecondaryInput(int i, Stream stream)
        {
            if (_secondaryInputs != null && _secondaryInputStreamConnectStatus != null && _secondaryInputStreamOperatorIndex != null && _secondaryInputStreamsInvertedIndex != null)
            {
                _secondaryInputs[i] = stream;
                _secondaryInputStreamsInvertedIndex.AddOrUpdate(_secondaryInputs[i], i, (key, value) => i);
                UpdateStreamStatus(_secondaryInputStreamConnectStatus, _secondaryInputStreamOperatorIndex, i, true);
                if (AreAllStreamsReady(_secondaryInputStreamConnectStatus, true))
                    ApplyOperatorSecondaryInput(_secondaryInputs);
            }
        }

        private  void ApplyOperatorSecondaryInput(Stream[] streams)
        {
            for (int i = 0; i < streams.Length; i++)
                UpdateStreamStatus(_secondaryInputStreamTriggerStatus, _secondaryInputStreamOperatorIndex, _secondaryInputStreamsInvertedIndex[streams[i]], true);
        }

        internal override void ApplyOperatorInput(Stream[] streams)
        {
            for (int i = 0; i < streams.Length; i++)
                UpdateStreamStatus(_inputStreamTriggerStatus, _inputStreamOperatorIndex, _inputStreamsInvertedIndex[streams[i]], true);

            StartMergeAndTransform();
        }

        private async void  StartMergeAndTransform()
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
                int splitIndex = Convert.ToInt32(_inputStreamOperatorIndex[i].Substring(_inputStreamOperatorIndex[i].Length - 1));
                inputSplitDatasets[splitIndex] = CreateDatasetFromInput(_inputStreamOperatorIndex[i], mergeTypes.SecondaryKeyType, mergeTypes.SecondaryPayloadType,
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

            if (AreAllStreamsReady(_inputStreamTriggerStatus, true) && AreAllStreamsReady(_secondaryInputStreamTriggerStatus, true)
                        && AreAllStreamsReady(_outputStreamTriggerStatus, true))
            {
                foreach (string operatorId in _inputStreamTriggerStatus.Keys)
                   _onCompletedInputs[operatorId].Set();

                foreach (string operatorId in _secondaryInputStreamTriggerStatus.Keys)
                   _onCompletedSecondaryInputs[operatorId].Set();

                foreach (string operatorId in _outputStreamTriggerStatus.Keys)
                   _onCompletedOutputs[operatorId].Set();
            }

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

        private Task<bool> StartProducerAsync(int streamIndex)
        {
            return Task.Factory.StartNew(() => {
                MethodInfo method = typeof(OperatorBase).GetMethod("StartProducer");
                MethodInfo generic = method.MakeGenericMethod(
                        new Type[] { _outputKeyType, _outputPayloadType, _outputDatasetType });
                generic.Invoke(this, new Object[] { new object[]{_cachedDatasets[_outputId]}, GetSiblingStreamsByStreamId(_outputStreamTriggerStatus,
                        _outputStreamOperatorIndex, _outputs, streamIndex), 1 });
                return true;
            });
        }

        private Task<bool> StartSplitProducerAsync(int streamIndex)
        {
            return Task.Factory.StartNew(() => {
                int splitIndex = Convert.ToInt32(_outputStreamOperatorIndex[streamIndex].Substring(_outputStreamOperatorIndex[streamIndex].Length - 1));
                object[] splitDatasets = (object[])_cachedDatasets[_outputId];
                MethodInfo method = typeof(OperatorBase).GetMethod("StartProducer");
                MethodInfo generic = method.MakeGenericMethod(
                        new Type[] { _outputKeyType, _outputPayloadType, _outputDatasetType });
                generic.Invoke(this, new Object[] { new object[]{splitDatasets[splitIndex]}, GetSiblingStreamsByStreamId(_outputStreamTriggerStatus,
                        _outputStreamOperatorIndex, _outputs, streamIndex), 1 });
                return true;
            });
        }

        private object CreateDatasetFromSecondaryInput(string operatorId, Type inputKeyType, Type inputPayloadType, Type inputDatasetType)
        {
            try
            {
                Stream[] streams = GetSiblingStreamsByOperatorId(_secondaryInputStreamTriggerStatus, operatorId, _secondaryInputs);
                MethodInfo method = typeof(OperatorBase).GetMethod("CreateDataset");
                MethodInfo generic = method.MakeGenericMethod(
                                        new Type[] { inputKeyType, inputPayloadType, inputDatasetType });
                return generic.Invoke(this, new Object[] { streams });
            }
            catch (Exception e)
            {
                throw new Exception("Error: Failed to create dataset from input!! " + e.ToString());
            }
        }

        internal override void ApplyOperatorOutput(Stream[] streams)
        {
            for (int i = 0; i < streams.Length; i++)
            {
                int currentIndex = i;
                Task.Run(() => StartProducerAfterTrigger(streams[currentIndex]));
            }
        }

        private async void StartProducerAfterTrigger(Stream stream)
        {
            CRATaskMessageType message = (CRATaskMessageType)(await stream.ReadInt32Async());
            if (message == CRATaskMessageType.READY)
                        StartProducerIfReady(stream);
        }

        private void StartProducerIfReady(Stream stream)
        {
            UpdateStreamStatus(_outputStreamTriggerStatus, _outputStreamOperatorIndex, _outputStreamsInvertedIndex[stream], true);
            if (AreAllStreamsReady(_outputStreamTriggerStatus, true))
            {
                if (AreAllStreamsReady(_inputStreamConnectStatus, true))
                {
                    if(_inputs != null)
                        for (int i = 0; i < _inputs.Length; i++)
                            _inputs[i].WriteInt32((int)CRATaskMessageType.READY);

                    if(_secondaryInputs != null)
                        for (int i = 0; i < _secondaryInputs.Length; i++)
                            _secondaryInputs[i].WriteInt32((int)CRATaskMessageType.READY);
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
                _secondaryInputs = new Stream[_secondaryInputsIds.Length];
            _secondaryInputStreamOperatorIndex = ToStreamOperatorIndex(((ShuffleTask)_task).SecondaryEndpointsDescriptor.FromInputs);
            _secondaryInputStreamConnectStatus = ToStreamStatus(_secondaryInputStreamOperatorIndex);
            _secondaryInputStreamTriggerStatus = ToStreamStatus(_secondaryInputStreamOperatorIndex);
            _secondaryInputStreamsInvertedIndex = new ConcurrentDictionary<Stream, int>();
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
            if (_onCompletedSecondaryInputs != null && _secondaryInputStreamOperatorIndex != null)
                _onCompletedSecondaryInputs[_secondaryInputStreamOperatorIndex[i]].WaitOne();
        }

        internal override void RemoveSecondaryInput(int i)
        {
            if (_secondaryInputs != null && _secondaryInputStreamConnectStatus != null && _secondaryInputStreamOperatorIndex != null)
            {
                UpdateStreamStatus(_secondaryInputStreamConnectStatus, _secondaryInputStreamOperatorIndex, i, false);
                if (AreSiblingStreamsReady(_secondaryInputStreamConnectStatus, _secondaryInputStreamOperatorIndex, i, false))
                    ResetAfterSecondaryInputCompletion(i);
            }
        }

        private void ResetAfterSecondaryInputCompletion(int i)
        {
            if (_onCompletedSecondaryInputs != null && _secondaryInputStreamOperatorIndex != null)
                _onCompletedSecondaryInputs[_secondaryInputStreamOperatorIndex[i]].Reset();
        }

    }
}
