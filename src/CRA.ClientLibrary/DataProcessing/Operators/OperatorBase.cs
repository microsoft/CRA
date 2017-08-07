using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Threading;

namespace CRA.ClientLibrary.DataProcessing
{
    public abstract class OperatorBase : ProcessBase
    {
        protected int _thisId;
        protected TaskBase _task;

        protected Stream[] _inputs;
        protected string[] _inputsIds;
        protected ConcurrentDictionary<int, string> _inputStreamOperatorIndex;
        protected ConcurrentDictionary<string, List<Tuple<int, bool>>> _inputStreamConnectStatus;
        protected ConcurrentDictionary<string, List<Tuple<int, bool>>> _inputStreamTriggerStatus;
        protected ConcurrentDictionary<Stream, int> _inputStreamsInvertedIndex;
        protected ConcurrentDictionary<string, ManualResetEvent> _onCompletedInputs;

        protected Stream[] _outputs;
        protected string[] _outputsIds;
        protected ConcurrentDictionary<int, string> _outputStreamOperatorIndex;
        protected ConcurrentDictionary<string, List<Tuple<int, bool>>> _outputStreamConnectStatus;
        protected ConcurrentDictionary<string, List<Tuple<int, bool>>> _outputStreamTriggerStatus;
        protected ConcurrentDictionary<Stream, int> _outputStreamsInvertedIndex;
        protected ConcurrentDictionary<string, ManualResetEvent> _onCompletedOutputs;

        public OperatorBase() : base() { }

        public override void Initialize(object processParameter)
        {
            PrepareOperatorParameter(processParameter);
            PrepareOperatorInput();
            PrepareOperatorOutput();
            
            InitializeOperator();
            base.Initialize(processParameter);
        }

        private void PrepareOperatorParameter(object processParameter)
        {
            if (processParameter is Tuple<int, ProduceTask>)
            {
                _thisId = ((Tuple<int, ProduceTask>)processParameter).Item1;
                _task = ((Tuple<int, ProduceTask>)processParameter).Item2;
            }
            else if (processParameter is Tuple<int, SubscribeTask>)
            {
                _thisId = ((Tuple<int, SubscribeTask>)processParameter).Item1;
                _task = ((Tuple<int, SubscribeTask>)processParameter).Item2;
            }
            else if (processParameter is Tuple<int, ShuffleTask>)
            {
                _thisId = ((Tuple<int, ShuffleTask>)processParameter).Item1;
                _task = ((Tuple<int, ShuffleTask>)processParameter).Item2;
            }
            else if (processParameter is Tuple<int, TaskBase>)
            {
                _thisId = ((Tuple<int, TaskBase>)processParameter).Item1;
                _task = ((Tuple<int, TaskBase>)processParameter).Item2;
            }
            else
                throw new InvalidCastException("Unsupported deployment task in CRA");
        }

        internal virtual void PrepareOperatorInput()
        {
            _inputsIds = ToEndpointsIds(_task.EndpointsDescriptor, false);
            if (_inputsIds != null && _inputsIds.Length > 0)
                _inputs = new Stream[_inputsIds.Length];
            _inputStreamOperatorIndex = ToStreamOperatorIndex(_task.EndpointsDescriptor.FromInputs);
            _inputStreamConnectStatus = ToStreamStatus(_inputStreamOperatorIndex);
            _inputStreamTriggerStatus = ToStreamStatus(_inputStreamOperatorIndex);
            _inputStreamsInvertedIndex = new ConcurrentDictionary<Stream, int>();
            _onCompletedInputs = new ConcurrentDictionary<string, ManualResetEvent>();
            foreach (var operatorId in _task.EndpointsDescriptor.FromInputs.Keys)
                _onCompletedInputs.AddOrUpdate(operatorId, new ManualResetEvent(false), (key, value) => new ManualResetEvent(false));
        }

        internal virtual void PrepareOperatorOutput()
        {
            _outputsIds = ToEndpointsIds(_task.EndpointsDescriptor, true);
            if (_outputsIds != null && _outputsIds.Length > 0)
                _outputs = new Stream[_outputsIds.Length];
            _outputStreamOperatorIndex = ToStreamOperatorIndex(_task.EndpointsDescriptor.ToOutputs);
            _outputStreamConnectStatus = ToStreamStatus(_outputStreamOperatorIndex);
            _outputStreamTriggerStatus = ToStreamStatus(_outputStreamOperatorIndex);
            _outputStreamsInvertedIndex = new ConcurrentDictionary<Stream, int>();
            _onCompletedOutputs = new ConcurrentDictionary<string, ManualResetEvent>();
            foreach (var operatorId in _task.EndpointsDescriptor.ToOutputs.Keys)
                _onCompletedOutputs.AddOrUpdate(operatorId, new ManualResetEvent(false), (key, value) => new ManualResetEvent(false));
        }

        
        public void StartProducer<TKey, TPayload, TDataset>(Object[] datasets, Stream[] streams, int activeStreams)
             where TDataset : IDataset<TKey, TPayload>
        {
            if (datasets.Length == activeStreams)
                for (int i = 0; i < datasets.Length; i++)
                    ((TDataset)datasets[i]).ToStream(streams[i]);
            else
                throw new InvalidOperationException();
        }
        

        protected object CreateDatasetFromInput(string operatorId, Type inputKeyType, Type inputPayloadType, Type inputDatasetType)
        {
            try
            {
                Stream[] streams = GetSiblingStreamsByOperatorId(_inputStreamTriggerStatus, operatorId, _inputs);
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

        public object CreateDataset<TKey, TPayload, TDataset>(Stream[] inputs)
            where TDataset : IDataset<TKey, TPayload>
        {
            if (inputs.Length == 1)
            {
                TDataset templateDataset = (TDataset)Activator.CreateInstance(typeof(TDataset));
                var compiledCreator = templateDataset.CreateFromStreamDeserializer().Compile();
                return (TDataset)compiledCreator(inputs[0]);
            }
            else
                throw new InvalidOperationException();
        }


        protected ConcurrentDictionary<string, List<Tuple<int, bool>>> ToStreamStatus(ConcurrentDictionary<int, string> streamOperatorIndex)
        {
            ConcurrentDictionary<string, List<Tuple<int, bool>>> statusIndex = new ConcurrentDictionary<string, List<Tuple<int, bool>>>();
            foreach (int streamId in streamOperatorIndex.Keys)
            {
                if (!statusIndex.ContainsKey(streamOperatorIndex[streamId]))
                    statusIndex.AddOrUpdate(streamOperatorIndex[streamId], new List<Tuple<int, bool>>(), (key, value) => new List<Tuple<int, bool>>());

                var streamItems = statusIndex[streamOperatorIndex[streamId]];
                streamItems.Add(new Tuple<int, bool>(streamId, false));
                statusIndex[streamOperatorIndex[streamId]] = streamItems;               
            }

            return statusIndex;
        }

        protected ConcurrentDictionary<int, string> ToStreamOperatorIndex(ConcurrentDictionary<string, int> operatorEndpoints, int startIndex = 0)
        {
            ConcurrentDictionary<int, string> streamOperatorMap = new ConcurrentDictionary<int, string>();

            int index = startIndex;
            foreach (string operatorId in operatorEndpoints.Keys)
            {
                for (int i = 0; i < operatorEndpoints[operatorId]; i++)
                    streamOperatorMap.AddOrUpdate(index + i, operatorId, (key, value) => operatorId);

                index = index + operatorEndpoints[operatorId];
            }

            return streamOperatorMap;
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

        internal void AddInput(int i, Stream stream)
        {
            if (_inputs != null && _inputStreamConnectStatus != null && _inputStreamOperatorIndex != null && _inputStreamsInvertedIndex != null)
            {
                _inputs[i] = stream;
                _inputStreamsInvertedIndex.AddOrUpdate(_inputs[i], i, (key, value) => i);
                UpdateStreamStatus(_inputStreamConnectStatus, _inputStreamOperatorIndex, i, true);
                if (AreAllStreamsReady(_inputStreamConnectStatus, true))
                    ApplyOperatorInput(_inputs);
            }
        }

        internal abstract void AddSecondaryInput(int i, Stream stream);

        internal abstract void ApplyOperatorInput(Stream[] streams);

        internal void AddOutput(int i, Stream stream)
        {
            if (_outputs != null  && _outputStreamConnectStatus != null && _outputStreamOperatorIndex != null && _outputStreamsInvertedIndex != null)
            {
                _outputs[i] = stream;
                _outputStreamsInvertedIndex.AddOrUpdate(_outputs[i], i, (key, value) => i);
                UpdateStreamStatus(_outputStreamConnectStatus, _outputStreamOperatorIndex, i, true);
                if (AreAllStreamsReady(_outputStreamConnectStatus, true))
                    ApplyOperatorOutput(_outputs);
            }
        }

        internal abstract void ApplyOperatorOutput(Stream[] streams);

        protected bool AreSiblingStreamsReady(ConcurrentDictionary<string, List<Tuple<int, bool>>> streamsStatusIndex, ConcurrentDictionary<int, string> streamOperatorIndex, int streamId, bool value)
        {
            var streamStatus = streamsStatusIndex[streamOperatorIndex[streamId]];
            foreach (var status in streamStatus)
                if (!(status.Item2 == value)) return false;
            return true;
        }

        protected bool AreAllStreamsReady(ConcurrentDictionary<string, List<Tuple<int, bool>>> streamsStatusIndex, bool value)
        {
            foreach (var key in streamsStatusIndex.Keys)
            {
                var streamStatus = streamsStatusIndex[key];
                foreach (var status in streamStatus)
                    if (!(status.Item2 == value)) return false;
            }
            return true;
        }

        protected Stream[] GetSiblingStreamsByStreamId(ConcurrentDictionary<string, List<Tuple<int, bool>>> streamsStatusIndex, ConcurrentDictionary<int, string> streamOperatorIndex, Stream[] endpointStreams, int streamId)
        {
            var streamStatus = streamsStatusIndex[streamOperatorIndex[streamId]].ToArray();
            Stream[] siblings = new Stream[streamStatus.Length];
            for (int i = 0; i < siblings.Length; i++)
                siblings[i] = endpointStreams[streamStatus[i].Item1];
            return siblings;
        }

        protected Stream[] GetSiblingStreamsByOperatorId(ConcurrentDictionary<string, List<Tuple<int, bool>>> streamsStatusIndex, string operatorId, Stream[] endpointStreams)
        {
            var streamStatus = streamsStatusIndex[operatorId].ToArray();
            Stream[] siblings = new Stream[streamStatus.Length];
            for (int i = 0; i < siblings.Length; i++)
                siblings[i] = endpointStreams[streamStatus[i].Item1];
            return siblings;
        }

        protected void UpdateStreamStatus(ConcurrentDictionary<string, List<Tuple<int, bool>>> streamsStatusIndex, ConcurrentDictionary<int, string> streamOperatorIndex, int streamId, bool status)
        {
            var streamStatus = streamsStatusIndex[streamOperatorIndex[streamId]];
            for (int i = 0; i < streamStatus.Count; i++)
            {
                if (streamStatus[i].Item1 == streamId)
                {
                    streamStatus[i] = new Tuple<int, bool>(streamId, status);
                    break;
                }
            }
            streamsStatusIndex[streamOperatorIndex[streamId]] = streamStatus;
        }

        internal void WaitForInputCompletion(int i)
        {
            if(_onCompletedInputs != null && _inputStreamOperatorIndex != null)
                            _onCompletedInputs[_inputStreamOperatorIndex[i]].WaitOne();
        }

        internal abstract void WaitForSecondaryInputCompletion(int i);

        internal void WaitForOutputCompletion(int i)
        {
            if(_onCompletedOutputs != null && _outputStreamOperatorIndex != null)
                            _onCompletedOutputs[_outputStreamOperatorIndex[i]].WaitOne();
        }

        internal void RemoveInput(int i)
        {
            if (_inputs != null && _inputStreamConnectStatus != null && _inputStreamOperatorIndex != null)
            {
                UpdateStreamStatus(_inputStreamConnectStatus, _inputStreamOperatorIndex, i, false);
                if (AreSiblingStreamsReady(_inputStreamConnectStatus, _inputStreamOperatorIndex, i, false))
                    ResetAfterInputCompletion(i);
            }
        }

        internal abstract void RemoveSecondaryInput(int i);

        internal void RemoveOutput(int i)
        {
            if (_outputs != null && _outputStreamConnectStatus != null && _outputStreamOperatorIndex != null)
            {
                UpdateStreamStatus(_outputStreamConnectStatus, _outputStreamOperatorIndex, i, false);
                if (AreSiblingStreamsReady(_outputStreamConnectStatus, _outputStreamOperatorIndex, i, false))
                   ResetAfterOutputCompletion(i);
            }
        }

        private void ResetAfterInputCompletion(int i)
        {
            if(_onCompletedInputs != null && _inputStreamOperatorIndex != null)
                            _onCompletedInputs[_inputStreamOperatorIndex[i]].Reset();
        }

        private void ResetAfterOutputCompletion(int i)
        {
            if(_onCompletedOutputs != null && _outputStreamOperatorIndex != null)
                            _onCompletedOutputs[_outputStreamOperatorIndex[i]].Reset();
        }
    }
}
