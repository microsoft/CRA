using System;
using System.IO;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace CRA.ClientLibrary.DataProcessing
{
    public class SubscribeOperator : OperatorBase
    {
        internal object _dataset;

        private string[] _outputsObservers;

        public SubscribeOperator() : base() {}

        internal override void InitializeOperator()
        {
            if (_inputsIds != null)
            {
                for (int i = 0; i < _inputsIds.Length; i++)
                    AddAsyncInputEndpoint(_inputsIds[i], new OperatorInput(this, i));
            }

            if (_outputsIds != null)
            {
                for (int i = 0; i < _outputsIds.Length; i++)
                    AddAsyncOutputEndpoint(_outputsIds[i], new OperatorOutput(this, i));
            }
        }
        
        internal override void ApplyOperatorInput(Stream[] streams){ }

        private async Task StartSubscribeIfReady(Stream stream)
        {
            UpdateStreamStatus(_inputStreamTriggerStatus, _inputStreamOperatorIndex, _inputStreamsInvertedIndex[stream], true);
            if (AreAllStreamsReady(_inputStreamTriggerStatus, true))
            {
                await Task.Run(() => ApplySubscribe());

                foreach (string operatorId in _outputStreamTriggerStatus.Keys)
                       _onCompletedOutputs[operatorId].Set();

                foreach (string operatorId in _inputStreamTriggerStatus.Keys)
                       _onCompletedInputs[operatorId].Set();
            }
        }


        private void ApplySubscribe()
        {
            _dataset = CreateDatasetFromInput(_task.InputIds.InputId1, _task.OperationTypes.OutputKeyType,
                                              _task.OperationTypes.OutputPayloadType, _task.OperationTypes.OutputDatasetType);

            MethodInfo method = typeof(SubscribeOperator).GetMethod("SubscribeObserver");
            MethodInfo generic = method.MakeGenericMethod(
                    new Type[] {_task.OperationTypes.OutputKeyType,
                                _task.OperationTypes.OutputPayloadType,
                                _task.OperationTypes.OutputDatasetType});
            generic.Invoke(this, new Object[] { _dataset, _outputsObservers[0] });
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
            _outputsObservers[_outputStreamsInvertedIndex[stream]] = Encoding.UTF8.GetString(stream.ReadByteArray());
            if (AreAllStreamsReady(_outputStreamTriggerStatus, true))
            {
                if (AreAllStreamsReady(_inputStreamConnectStatus, true))
                {
                    for (int i = 0; i < _inputs.Length; i++)
                    {
                        int currentIndex = i;
                        Task.Run(() => StartSubscribeIfReady(_inputs[currentIndex]));
                    }

                    for (int i = 0; i < _inputs.Length; i++)
                        _inputs[i].WriteInt32((int)CRATaskMessageType.READY);
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

        public override void Dispose()
        {
            base.Dispose();
        }
    }
}
