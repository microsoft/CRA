using System;
using System.Reflection;
using System.Linq.Expressions;
using System.IO;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace CRA.ClientLibrary.DataProcessing
{
    public class ProducerOperator : OperatorBase
    {
        internal Dictionary<string, object> _cachedDatasets;

        private Type _outputKeyType;
        private Type _outputPayloadType;
        private Type _outputDatasetType;
        private string _outputId;

        public ProducerOperator() : base()
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

            CreateAndTransformDataset();

            if (_outputsIds != null)
            {
                for (int i = 0; i < _outputsIds.Length; i++)
                    AddAsyncOutputEndpoint(_outputsIds[i], new OperatorOutput(this, i));
            }
        }

        public void CreateAndTransformDataset()
        {
            var produceTask = (ProduceTask)_task;

            MethodInfo method = typeof(ProducerOperator).GetMethod("CreateDatasetFromExpression");
            MethodInfo generic = method.MakeGenericMethod(
                                    new Type[] {produceTask.OperationTypes.OutputKeyType,
                                                produceTask.OperationTypes.OutputPayloadType,
                                                produceTask.OperationTypes.OutputDatasetType});
            object[] arguments = new Object[] { produceTask.DataProducer };
            _cachedDatasets[produceTask.OutputId] = generic.Invoke(this, arguments);

            _outputKeyType = produceTask.OperationTypes.OutputKeyType;
            _outputPayloadType = produceTask.OperationTypes.OutputPayloadType;
            _outputDatasetType = produceTask.OperationTypes.OutputDatasetType;
            _outputId = produceTask.OutputId;

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
                            dataset2 = CreateDatasetFromInput(dataset2Id, binaryTransformTypes.SecondaryKeyType,
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
        }

        public object CreateDatasetFromExpression<TKey, TPayload, TDataset>(string producerExpression)
            where TDataset : IDataset<TKey, TPayload>
        {
            var producer = (Expression<Func<int, TDataset>>)SerializationHelper.Deserialize(producerExpression);
            var compiledProducer = producer.Compile();
            return compiledProducer(_thisId);
        }

        internal override void ApplyOperatorInput(Stream[] streams)
        {
            for (int i = 0; i < streams.Length; i++)
                UpdateStreamStatus(_inputStreamTriggerStatus, _inputStreamOperatorIndex, _inputStreamsInvertedIndex[streams[i]], true);
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

                if (isSuccess)
                {
                    if (AreAllStreamsReady(_inputStreamTriggerStatus, true))
                        foreach (string operatorId in _inputStreamTriggerStatus.Keys)
                            _onCompletedInputs[operatorId].Set();

                    foreach (string operatorId in _outputStreamTriggerStatus.Keys)
                           _onCompletedOutputs[operatorId].Set();
                }
            }
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

        internal override void PrepareOperatorInput()
        {
            base.PrepareOperatorInput();
        }

        internal override void PrepareOperatorOutput()
        {
            base.PrepareOperatorOutput();
        }

        internal override void AddSecondaryInput(int i, Stream stream)
        {
            throw new NotImplementedException();
        }

        internal override void WaitForSecondaryInputCompletion(int i)
        {
            throw new NotImplementedException();
        }

        internal override void RemoveSecondaryInput(int i)
        {
            throw new NotImplementedException();
        }
    }
}
