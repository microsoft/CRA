using CRA.ClientLibrary.DataProvider;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace CRA.ClientLibrary.DataProcessing
{
    public abstract class OperatorBase : VertexBase
    {
        protected int _thisId;
        protected TaskBase _task;

        protected IEndpointContent[] _inputs;
        protected string[] _inputsIds;
        protected ConcurrentDictionary<int, string> _inputEndpointOperatorIndex;
        protected ConcurrentDictionary<string, List<Tuple<int, bool>>> _inputEndpointConnectStatus;
        protected ConcurrentDictionary<string, List<Tuple<int, bool>>> _inputEndpointTriggerStatus;
        protected ConcurrentDictionary<IEndpointContent, int> _inputEndpointInvertedIndex;
        protected ConcurrentDictionary<string, ManualResetEvent> _onCompletedInputs;
        protected ConcurrentDictionary<Tuple<string, string>, Tuple<string, string, bool>> _fromToConnections;

        protected IEndpointContent[] _outputs;
        protected string[] _outputsIds;
        protected ConcurrentDictionary<int, string> _outputEndpointOperatorIndex;
        protected ConcurrentDictionary<string, List<Tuple<int, bool>>> _outputEndpointConnectStatus;
        protected ConcurrentDictionary<string, List<Tuple<int, bool>>> _outputEndpointTriggerStatus;
        protected ConcurrentDictionary<IEndpointContent, int> _outputEndpointInvertedIndex;
        protected ConcurrentDictionary<string, ManualResetEvent> _onCompletedOutputs;
        protected ConcurrentDictionary<Tuple<string, string>, Tuple<string, string, bool>> _toFromConnections;

        protected CRAClientLibrary _craClient;

        private System.Object _applyInputLock = new System.Object();
        private bool _isInputApplied = false;

        private System.Object _applyOutputLock = new System.Object();
        private bool _isOutputApplied = false;

        public OperatorBase(IDataProvider dataProvider) : base()
        {
            _craClient = new CRAClientLibrary(dataProvider);
        }

        public override Task InitializeAsync(object vertexParameter)
        {
            PrepareOperatorParameter(vertexParameter);
            PrepareAllConnectionsMap();
            PrepareOperatorInput();
            PrepareOperatorOutput();
            
            InitializeOperator();
            return base.InitializeAsync(vertexParameter);
        }

        private void PrepareOperatorParameter(object vertexParameter)
        {
            if (vertexParameter is Tuple<int, ProduceTask>)
            {
                _thisId = ((Tuple<int, ProduceTask>)vertexParameter).Item1;
                _task = ((Tuple<int, ProduceTask>)vertexParameter).Item2;
            }
            else if (vertexParameter is Tuple<int, SubscribeTask>)
            {
                _thisId = ((Tuple<int, SubscribeTask>)vertexParameter).Item1;
                _task = ((Tuple<int, SubscribeTask>)vertexParameter).Item2;
            }
            else if (vertexParameter is Tuple<int, ShuffleTask>)
            {
                _thisId = ((Tuple<int, ShuffleTask>)vertexParameter).Item1;
                _task = ((Tuple<int, ShuffleTask>)vertexParameter).Item2;
            }
            else if (vertexParameter is Tuple<int, TaskBase>)
            {
                _thisId = ((Tuple<int, TaskBase>)vertexParameter).Item1;
                _task = ((Tuple<int, TaskBase>)vertexParameter).Item2;
            }
            else
                throw new InvalidCastException("Unsupported deployment task in CRA");
        }

        private void PrepareAllConnectionsMap()
        {
            _fromToConnections = new ConcurrentDictionary<Tuple<string, string>, Tuple<string, string, bool>>();
            _toFromConnections = new ConcurrentDictionary<Tuple<string, string>, Tuple<string, string, bool>>();

            var connectionsMap = _task.VerticesConnectionsMap;
            foreach (var connectionsListKey in connectionsMap.Keys)
            {
                var connectionsList = connectionsMap[connectionsListKey];
                foreach (var connection in connectionsList)
                {
                    var fromTuple = new Tuple<string, string>(connection.FromVertex, connection.FromEndpoint);
                    var toTuple = new Tuple<string, string, bool>(connection.ToVertex, connection.ToEndpoint, connection.IsOnSameCRAInstance);
                    _fromToConnections.AddOrUpdate(fromTuple, toTuple, (key, value) => toTuple);

                    fromTuple = new Tuple<string, string>(connection.ToVertex, connection.ToEndpoint);
                    toTuple = new Tuple<string, string, bool>(connection.FromVertex, connection.FromEndpoint, connection.IsOnSameCRAInstance);
                    _toFromConnections.AddOrUpdate(fromTuple, toTuple, (key, value) => toTuple);
                }
            }
        }

        internal virtual void PrepareOperatorInput()
        {
            _inputsIds = ToEndpointsIds(_task.EndpointsDescriptor, false);
            if (_inputsIds != null && _inputsIds.Length > 0)
                _inputs = new IEndpointContent[_inputsIds.Length];
            _inputEndpointOperatorIndex = ToEndpointOperatorIndex(_task.EndpointsDescriptor.FromInputs);
            _inputEndpointConnectStatus = ToEndpointStatus(_inputEndpointOperatorIndex);
            _inputEndpointTriggerStatus = ToEndpointStatus(_inputEndpointOperatorIndex);
            _inputEndpointInvertedIndex = new ConcurrentDictionary<IEndpointContent, int>();
            _onCompletedInputs = new ConcurrentDictionary<string, ManualResetEvent>();
            foreach (var operatorId in _task.EndpointsDescriptor.FromInputs.Keys)
                _onCompletedInputs.AddOrUpdate(operatorId, new ManualResetEvent(false), (key, value) => new ManualResetEvent(false));
        }

        internal virtual void PrepareOperatorOutput()
        {
            _outputsIds = ToEndpointsIds(_task.EndpointsDescriptor, true);
            if (_outputsIds != null && _outputsIds.Length > 0)
                _outputs = new IEndpointContent[_outputsIds.Length];
            _outputEndpointOperatorIndex = ToEndpointOperatorIndex(_task.EndpointsDescriptor.ToOutputs);
            _outputEndpointConnectStatus = ToEndpointStatus(_outputEndpointOperatorIndex);
            _outputEndpointTriggerStatus = ToEndpointStatus(_outputEndpointOperatorIndex);
            _outputEndpointInvertedIndex = new ConcurrentDictionary<IEndpointContent, int>();
            _onCompletedOutputs = new ConcurrentDictionary<string, ManualResetEvent>();
            foreach (var operatorId in _task.EndpointsDescriptor.ToOutputs.Keys)
                _onCompletedOutputs.AddOrUpdate(operatorId, new ManualResetEvent(false), (key, value) => new ManualResetEvent(false));
        }

        
        public void StartProducer<TKey, TPayload, TDataset>(Object[] datasets, int[] outputIndices, int activeEndpoints)
             where TDataset : IDataset<TKey, TPayload>
        {
            if (datasets.Length == activeEndpoints)
                for (int i = 0; i < outputIndices.Length; i++)
                {
                    if (_outputs[outputIndices[i]] as StreamEndpoint != null)
                        ((TDataset)datasets[i]).ToStream(((StreamEndpoint)_outputs[outputIndices[i]]).Stream);
                    else
                    {
                        ((ObjectEndpoint)_outputs[outputIndices[i]]).OwningOutputEndpoint.InputEndpoint.Dataset = ((TDataset)datasets[i]).ToObject();
                        ((ObjectEndpoint)_outputs[outputIndices[i]]).OwningOutputEndpoint.InputEndpoint.EndpointContent.FireTrigger.Set(); 
                    }
                }
                    
            else
                throw new InvalidOperationException();
        }

        protected object CreateDatasetFromInput(string operatorId, Type inputKeyType, Type inputPayloadType, Type inputDatasetType)
        {
            try
            {
                int[] endpointsIndices = GetSiblingInputEndpointsByOperatorId(_inputEndpointTriggerStatus, operatorId);
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

        public object CreateDataset<TKey, TPayload, TDataset>(int[] inputsIndices)
            where TDataset : IDataset<TKey, TPayload>
        {
            if (inputsIndices.Length == 1)
            {
                if (_inputs[inputsIndices[0]] as StreamEndpoint != null)
                {
                    TDataset templateDataset = (TDataset)Activator.CreateInstance(typeof(TDataset));
                    var compiledCreator = templateDataset.CreateFromStreamDeserializer().Compile();
                    return (TDataset)compiledCreator(((StreamEndpoint)_inputs[inputsIndices[0]]).Stream);
                }
                else
                {
                    ((ObjectEndpoint)_inputs[inputsIndices[0]]).OnReceivedFireMessage();
                    ((ObjectEndpoint)_inputs[inputsIndices[0]]).FireTrigger.Reset();
                    return (TDataset)(((ObjectEndpoint)_inputs[inputsIndices[0]]).OwningInputEndpoint.Dataset);
                }

            }
            else
                throw new InvalidOperationException();
        }


        protected ConcurrentDictionary<string, List<Tuple<int, bool>>> ToEndpointStatus(ConcurrentDictionary<int, string> endpointOperatorIndex)
        {
            ConcurrentDictionary<string, List<Tuple<int, bool>>> statusIndex = new ConcurrentDictionary<string, List<Tuple<int, bool>>>();
            foreach (int endpointId in endpointOperatorIndex.Keys)
            {
                if (!statusIndex.ContainsKey(endpointOperatorIndex[endpointId]))
                    statusIndex.AddOrUpdate(endpointOperatorIndex[endpointId], new List<Tuple<int, bool>>(), (key, value) => new List<Tuple<int, bool>>());

                var endpointItems = statusIndex[endpointOperatorIndex[endpointId]];
                endpointItems.Add(new Tuple<int, bool>(endpointId, false));
                statusIndex[endpointOperatorIndex[endpointId]] = endpointItems;               
            }

            return statusIndex;
        }

        protected ConcurrentDictionary<int, string> ToEndpointOperatorIndex(ConcurrentDictionary<string, int> operatorEndpoints, int startIndex = 0)
        {
            ConcurrentDictionary<int, string> endpointOperatorMap = new ConcurrentDictionary<int, string>();

            int index = startIndex;
            foreach (string operatorId in operatorEndpoints.Keys)
            {
                for (int i = 0; i < operatorEndpoints[operatorId]; i++)
                    endpointOperatorMap.AddOrUpdate(index + i, operatorId, (key, value) => operatorId);

                index = index + operatorEndpoints[operatorId];
            }

            return endpointOperatorMap;
        }

        protected string[] ToEndpointsIds(OperatorEndpointsDescriptor endpointDescriptor, bool isToOutput)
        {
            List<string> endpointsIds = new List<string>();
            if (isToOutput)
            {
                foreach (string endpointId in endpointDescriptor.ToOutputs.Keys)
                    endpointsIds.AddRange(
                        OperatorUtils.PrepareOutputEndpointsIdsForOperator(endpointId, endpointDescriptor));
            }
            else
            {
                foreach (string endpointId in endpointDescriptor.FromInputs.Keys)
                    endpointsIds.AddRange(
                        OperatorUtils.PrepareInputEndpointsIdsForOperator(endpointId, endpointDescriptor));
            }
            
            return endpointsIds.ToArray();
        }

        internal abstract void InitializeOperator();

        internal void AddInput(int i, ref IEndpointContent endpoint)
        {
            if (_inputs != null && _inputEndpointConnectStatus != null && _inputEndpointOperatorIndex != null && _inputEndpointInvertedIndex != null)
            {
                _inputs[i] = endpoint;
                _inputEndpointInvertedIndex.AddOrUpdate(_inputs[i], i, (key, value) => i);
                UpdateEndpointStatus(_inputEndpointConnectStatus, _inputEndpointOperatorIndex, i, true);
                if (AreAllEndpointsReady(_inputEndpointConnectStatus, true))
                {
                    lock (_applyInputLock)
                    {
                        if (!_isInputApplied)
                        {
                            int[] inputIndices = new int[_inputs.Length];
                            for (int j = 0; j < _inputs.Length; j++)
                                inputIndices[j] = j;

                            ApplyOperatorInput(inputIndices);
                            _isInputApplied = true;
                        }
                    }
                    
                }
            }
        }

        internal abstract void AddSecondaryInput(int i, ref IEndpointContent endpoint);

        internal abstract void ApplyOperatorInput(int[] inputIndices);

        internal void AddOutput(int i, ref IEndpointContent endpoint)
        {
            if (_outputs != null  && _outputEndpointConnectStatus != null && _outputEndpointOperatorIndex != null && _outputEndpointInvertedIndex != null)
            {
                _outputs[i] = endpoint;
                _outputEndpointInvertedIndex.AddOrUpdate(_outputs[i], i, (key, value) => i);
                UpdateEndpointStatus(_outputEndpointConnectStatus, _outputEndpointOperatorIndex, i, true);
                if (AreAllEndpointsReady(_outputEndpointConnectStatus, true))
                {
                    lock (_applyOutputLock)
                    {
                        if (!_isOutputApplied)
                        {
                            int[] outputIndices = new int[_outputs.Length];
                            for (int j = 0; j < _outputs.Length; j++)
                                outputIndices[j] = j;

                            ApplyOperatorOutput(outputIndices);
                            _isOutputApplied = true;
                        }
                    }
                }
            }
        }

        internal abstract void ApplyOperatorOutput(int[] outputIndices);

        protected bool AreSiblingEndpointsReady(ConcurrentDictionary<string, List<Tuple<int, bool>>> endpointsStatusIndex, ConcurrentDictionary<int, string> endpointOperatorIndex, int endpointId, bool value)
        {
            var endpointStatus = endpointsStatusIndex[endpointOperatorIndex[endpointId]];
            foreach (var status in endpointStatus)
                if (!(status.Item2 == value)) return false;
            return true;
        }

        protected bool AreAllEndpointsReady(ConcurrentDictionary<string, List<Tuple<int, bool>>> endpointsStatusIndex, bool value)
        {
            foreach (var key in endpointsStatusIndex.Keys)
            {
                var endpointStatus = endpointsStatusIndex[key];
                foreach (var status in endpointStatus)
                    if (!(status.Item2 == value)) return false;
            }
            return true;
        }

        protected int[] GetSiblingEndpointsByEndpointId(ConcurrentDictionary<string, List<Tuple<int, bool>>> endpointsStatusIndex, ConcurrentDictionary<int, string> endpointOperatorIndex, int endpointId)
        {
            var endpointStatus = endpointsStatusIndex[endpointOperatorIndex[endpointId]].ToArray();
            int[] siblings = new int[endpointStatus.Length];
            for (int i = 0; i < siblings.Length; i++)
                siblings[i] = endpointStatus[i].Item1;
            return siblings;
        }

        protected int[] GetSiblingInputEndpointsByOperatorId(ConcurrentDictionary<string, List<Tuple<int, bool>>> endpointsStatusIndex, string operatorId)
        {
            var endpointStatus = endpointsStatusIndex[operatorId].ToArray();
            int[] siblings = new int[endpointStatus.Length];
            for (int i = 0; i < siblings.Length; i++)
                siblings[i] = endpointStatus[i].Item1;
            return siblings;
        }

        protected void UpdateEndpointStatus(ConcurrentDictionary<string, List<Tuple<int, bool>>> endpointsStatusIndex, ConcurrentDictionary<int, string> endpointOperatorIndex, int endpointId, bool status)
        {
            var endpointStatus = endpointsStatusIndex[endpointOperatorIndex[endpointId]];
            for (int i = 0; i < endpointStatus.Count; i++)
            {
                if (endpointStatus[i].Item1 == endpointId)
                {
                    endpointStatus[i] = new Tuple<int, bool>(endpointId, status);
                    break;
                }
            }
            endpointsStatusIndex[endpointOperatorIndex[endpointId]] = endpointStatus;
        }

        internal void WaitForInputCompletion(int i)
        {
            if(_onCompletedInputs != null && _inputEndpointOperatorIndex != null)
                            _onCompletedInputs[_inputEndpointOperatorIndex[i]].WaitOne();
        }

        internal abstract void WaitForSecondaryInputCompletion(int i);

        internal void WaitForOutputCompletion(int i)
        {
            if(_onCompletedOutputs != null && _outputEndpointOperatorIndex != null)
                            _onCompletedOutputs[_outputEndpointOperatorIndex[i]].WaitOne();
        }

        internal void RemoveInput(int i)
        {
            if (_inputs != null && _inputEndpointConnectStatus != null && _inputEndpointOperatorIndex != null)
            {
                UpdateEndpointStatus(_inputEndpointConnectStatus, _inputEndpointOperatorIndex, i, false);
                if (AreSiblingEndpointsReady(_inputEndpointConnectStatus, _inputEndpointOperatorIndex, i, false))
                    ResetAfterInputCompletion(i);
            }
        }

        internal abstract void RemoveSecondaryInput(int i);

        internal void RemoveOutput(int i)
        {
            if (_outputs != null && _outputEndpointConnectStatus != null && _outputEndpointOperatorIndex != null)
            {
                UpdateEndpointStatus(_outputEndpointConnectStatus, _outputEndpointOperatorIndex, i, false);
                if (AreSiblingEndpointsReady(_outputEndpointConnectStatus, _outputEndpointOperatorIndex, i, false))
                   ResetAfterOutputCompletion(i);
            }
        }

        private void ResetAfterInputCompletion(int i)
        {
            if(_onCompletedInputs != null && _inputEndpointOperatorIndex != null)
                            _onCompletedInputs[_inputEndpointOperatorIndex[i]].Reset();
        }

        private void ResetAfterOutputCompletion(int i)
        {
            if(_onCompletedOutputs != null && _outputEndpointOperatorIndex != null)
                            _onCompletedOutputs[_outputEndpointOperatorIndex[i]].Reset();
        }
    }
}
