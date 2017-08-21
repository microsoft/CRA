using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace CRA.ClientLibrary.DataProcessing
{
    //NOTE: Currently, subscribe is working properly with one output endpoint only
    public class SubscribeOperator : OperatorBase
    {
        internal Dictionary<string, object> _cachedDatasets;

        private string[] _outputsObservers;

        private System.Object _produceIfReadyLock = new System.Object();
        private bool _isProduceIfReadyApplied = false;

        private Stopwatch _watch = new Stopwatch();

        public SubscribeOperator() : base()
        {
            _cachedDatasets = new Dictionary<string, object>();
        }

        internal override void InitializeOperator()
        {
            IProcess thisOperator = this;
            if (_inputsIds != null)
                for (int i = 0; i < _inputsIds.Length; i++)
                {
                    var fromTuple = _toFromConnections[new Tuple<string, string>(ProcessName, _inputsIds[i])];
                    if (fromTuple.Item3)
                        AddAsyncInputEndpoint(_inputsIds[i], new OperatorFusableInput(ref thisOperator, i));
                    else
                        AddAsyncInputEndpoint(_inputsIds[i], new OperatorInput(ref thisOperator, i));
                }

            if (_outputsIds != null)
                for (int i = 0; i < _outputsIds.Length; i++)
                        AddAsyncOutputEndpoint(_outputsIds[i], new OperatorOutput(ref thisOperator, i));
        }
        
        internal override void ApplyOperatorInput(int[] inputIndices){ }

        private async Task StartSubscribeIfReady(int inputIndex)
        {
            UpdateEndpointStatus(_inputEndpointTriggerStatus, _inputEndpointOperatorIndex, inputIndex, true);
            if (AreAllEndpointsReady(_inputEndpointTriggerStatus, true))
            {
                await Task.Run(() => ApplySubscribe());
                await ContinueOrRelease();
            }
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

        private async Task<bool> ContinueOrRelease()
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
                    throw new InvalidCastException();
                }
            }

            if (AreAllFlagsTrue(releaseFlags))
            {
                foreach (string operatorId in _outputEndpointTriggerStatus.Keys)
                    _onCompletedOutputs[operatorId].Set();

                foreach (string operatorId in _inputEndpointTriggerStatus.Keys)
                    _onCompletedInputs[operatorId].Set();

                for (int i = 0; i < _inputs.Length; i++)
                {
                    if (_inputs[i] as StreamEndpoint != null)
                        ((StreamEndpoint)_inputs[i]).Stream.WriteInt32((int)CRATaskMessageType.RELEASE);
                    else
                        ((ObjectEndpoint)_inputs[i]).ReleaseTrigger.Set();
                }
            }
            else
            {
                _isProduceIfReadyApplied = false;
                for (int i = 0; i < _outputs.Length; i++)
                {
                    int currentIndex = i;
                    Task.Run(() => ContinueProducerAfterTrigger(currentIndex));
                }
                
            }
            return true;
        }
        
        private async void ContinueProducerAfterTrigger(int outputIndex)
        {
            if (_outputs[outputIndex] as StreamEndpoint != null)
                StartProducerIfReady(outputIndex);
            else
            {
                throw new InvalidCastException();
            }
        }
        
        private void ApplySubscribe()
        {
            object dataset = CreateDatasetFromInput(_task.InputIds.InputId1, _task.OperationTypes.OutputKeyType,
                                          _task.OperationTypes.OutputPayloadType, _task.OperationTypes.OutputDatasetType);

            if (!_cachedDatasets.ContainsKey(_task.InputIds.InputId1))
            {
                _cachedDatasets.Add(_task.InputIds.InputId1, dataset);
            }
            else
            {
                _cachedDatasets[_task.InputIds.InputId1] = dataset;
            }

            _watch.Reset();
            _watch.Start();

            MethodInfo method = typeof(SubscribeOperator).GetMethod("SubscribeObserver");
            MethodInfo generic = method.MakeGenericMethod(
                    new Type[] {_task.OperationTypes.OutputKeyType,
                                _task.OperationTypes.OutputPayloadType,
                                _task.OperationTypes.OutputDatasetType});
            generic.Invoke(this, new Object[] { _cachedDatasets[_task.InputIds.InputId1], _outputsObservers[0] });

            _watch.Stop();
            Console.WriteLine("Execution time in millisec of query: " + _watch.ElapsedMilliseconds);
        }

        public void SubscribeObserver<TKey, TPayload, TDataset>(object dataset, string observerExpression)
            where TDataset : IDataset<TKey, TPayload>
        {
            Expression observer = SerializationHelper.Deserialize(observerExpression);
            Delegate compiledObserver = Expression.Lambda(observer).Compile();
            Delegate observerConstructor = (Delegate)compiledObserver.DynamicInvoke();
            object observerObject = observerConstructor.DynamicInvoke();
            ((TDataset)dataset).Subscribe(observerObject);
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
                            StartProducerIfReady(outputIndex);
            }
            else
            {
                throw new InvalidCastException();
            }
        }

        private void StartProducerIfReady(int outputIndex)
        {
            UpdateEndpointStatus(_outputEndpointTriggerStatus, _outputEndpointOperatorIndex, outputIndex, true);
            if (_outputs[outputIndex] as StreamEndpoint != null)
                _outputsObservers[outputIndex] =
                            Encoding.UTF8.GetString(((StreamEndpoint)_outputs[outputIndex]).Stream.ReadByteArray());
            else
            {
                throw new InvalidCastException();
            }

            if (AreAllEndpointsReady(_outputEndpointTriggerStatus, true))
            {
                if (AreAllEndpointsReady(_inputEndpointConnectStatus, true))
                {
                    lock (_produceIfReadyLock)
                    {
                        if (!_isProduceIfReadyApplied)
                        {
                            for (int i = 0; i < _inputs.Length; i++)
                            {
                                int currentIndex = i;
                                Task.Run(() => StartSubscribeIfReady(currentIndex));
                            }

                            for (int i = 0; i < _inputs.Length; i++)
                            {
                                if (_inputs[i] as StreamEndpoint != null)
                                    ((StreamEndpoint)_inputs[i]).Stream.WriteInt32((int)CRATaskMessageType.READY);
                                else 
                                    ((ObjectEndpoint)_inputs[i]).ReadyTrigger.Set();
                            }

                           //_isProduceIfReadyApplied = true;
                        }
                    }
                }
            }
        }

        internal override void PrepareOperatorInput()
        {
            base.PrepareOperatorInput();
        }

        internal override void PrepareOperatorOutput()
        {
            base.PrepareOperatorOutput();
            if (_outputsIds != null)
                _outputsObservers = new string[_outputsIds.Length];
        }

        internal override void AddSecondaryInput(int i, ref IEndpointContent endpoint)
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

        public override void Dispose()
        {
            base.Dispose();
        }
    }
}
