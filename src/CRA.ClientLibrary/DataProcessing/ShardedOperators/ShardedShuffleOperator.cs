using CRA.DataProvider;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;


namespace CRA.ClientLibrary.DataProcessing
{
    public class ShardedShuffleOperator : ShardedOperatorBase
    {
        internal Type _outputKeyType;
        internal Type _outputPayloadType;
        internal Type _outputDatasetType;
        internal string _outputId;

        internal Dictionary<int, object[]> _inputSplitDatasets;

        internal CountdownEvent _deployShuffleInput;  
        internal CountdownEvent _deployShuffleOutput; 

        internal CountdownEvent _runShuffleInput;  
        internal CountdownEvent _runShuffleOutput;

        internal Dictionary<string, CountdownEvent> _startCreatingSecondaryDatasets;
        internal Dictionary<string, CountdownEvent> _finishCreatingSecondaryDatasets;
        internal Dictionary<string, BinaryOperatorTypes> _binaryOperatorTypes;

        public ShardedShuffleOperator() : base()
        {
            _inputSplitDatasets = new Dictionary<int, object[]>();

            _startCreatingSecondaryDatasets = new Dictionary<string, CountdownEvent>();
            _finishCreatingSecondaryDatasets = new Dictionary<string, CountdownEvent>();
            _binaryOperatorTypes = new Dictionary<string, BinaryOperatorTypes>();
        }

        internal override void InitializeOperator(int shardId, ShardingInfo shardingInfo)
        {
            _hasSplittedOutput = HasSplittedOutput();
            string[] toEndpoints = GetEndpointNamesForVertex(VertexName.Split('$')[0], _toFromConnections);
            string[] fromEndpoints = GetEndpointNamesForVertex(VertexName.Split('$')[0], _fromToConnections);

            int secondaryOutputsCount = 0;
            int ordinaryOutputSCount = 0;
            foreach (var fromEndpoint in fromEndpoints)
            {
                var toTuple = _fromToConnections[new Tuple<string, string>(VertexName.Split('$')[0], fromEndpoint)];
                if (toTuple.Item4)
                    secondaryOutputsCount++;
                else
                    ordinaryOutputSCount++;
            }
            int deployShuffleInputCount = secondaryOutputsCount;
            if (_hasSplittedOutput)
                deployShuffleInputCount += shardingInfo.AllShards.Length;
            else
                deployShuffleInputCount += ordinaryOutputSCount;
            _deployShuffleInput = new CountdownEvent(deployShuffleInputCount);
            _runShuffleInput = new CountdownEvent(deployShuffleInputCount);

            int secondaryInputsCount = 0;
            foreach (var toEndpoint in toEndpoints)
            {
                var fromTuple = _toFromConnections[new Tuple<string, string>(VertexName.Split('$')[0], toEndpoint)];
                if (fromTuple.Item4) secondaryInputsCount++;
            }
            _deployShuffleOutput = new CountdownEvent(shardingInfo.AllShards.Length + secondaryInputsCount);
            _runShuffleOutput = new CountdownEvent(shardingInfo.AllShards.Length + secondaryInputsCount);

            _inputSplitDatasets[shardId] = new object[shardingInfo.AllShards.Length];

            foreach (var toEndpoint in toEndpoints)
            {
                var fromTuple = _toFromConnections[new Tuple<string, string>(VertexName.Split('$')[0], toEndpoint)];
                if (!fromTuple.Item4)
                    AddAsyncInputEndpoint(toEndpoint, new ShardedShuffleInput(this, shardId, shardingInfo.AllShards.Length, toEndpoint));
                else
                {
                    _startCreatingSecondaryDatasets[fromTuple.Item1] = new CountdownEvent(1);
                    _finishCreatingSecondaryDatasets[fromTuple.Item1] = new CountdownEvent(1);
                    AddAsyncInputEndpoint(toEndpoint, new ShardedShuffleSecondaryInput(this, shardId, shardingInfo.AllShards.Length, toEndpoint));
                }
            }

            foreach (var fromEndpoint in fromEndpoints)
            {
                var toTuple = _fromToConnections[new Tuple<string, string>(VertexName.Split('$')[0], fromEndpoint)];
                if (!toTuple.Item4)
                    AddAsyncOutputEndpoint(fromEndpoint, new ShardedShuffleOutput(this, shardId, shardingInfo.AllShards.Length, fromEndpoint));
                else
                    AddAsyncOutputEndpoint(fromEndpoint, new ShardedShuffleSecondaryOutput(this, shardId, shardingInfo.AllShards.Length, fromEndpoint));
            }
        }

        internal override bool HasSplittedOutput()
        {
            if (_task.Transforms != null && _task.Transforms.Length != 0 &&
                          _task.TransformsOperations[_task.Transforms.Length - 1] == OperatorType.MoveSplit.ToString())
                return true;
            else
                return false;
        }
    }
}
