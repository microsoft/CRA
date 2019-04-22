using CRA.ClientLibrary.DataProvider;
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

        public ShardedShuffleOperator() : base()
        {
            _inputSplitDatasets = new Dictionary<int, object[]>();
        }

        internal override void InitializeOperator(int shardId, ShardingInfo shardingInfo)
        {
            _hasSplittedOutput = HasSplittedOutput();

            if (_hasSplittedOutput)
            {
                _deployShuffleInput = new CountdownEvent(shardingInfo.AllShards.Length);
                _runShuffleInput = new CountdownEvent(shardingInfo.AllShards.Length);
            }
            else
            {
                _deployShuffleInput = new CountdownEvent(1);
                _runShuffleInput = new CountdownEvent(1);
            }

            _deployShuffleOutput = new CountdownEvent(shardingInfo.AllShards.Length);
            _runShuffleOutput = new CountdownEvent(shardingInfo.AllShards.Length);

            _inputSplitDatasets[shardId] = new object[shardingInfo.AllShards.Length];

            //TODO: check for secondary input how to get it

            string toEndpoint = GetEndpointNameForVertex(VertexName.Split('$')[0], _toFromConnections);
            /*var fromTuple = _toFromConnections[new Tuple<string, string>(VertexName.Split('$')[0], toEndpoint)];
            if (fromTuple.Item3)
                throw new NotImplementedException("Shared memory endpoints are not supported yet!!");
            else*/
            AddAsyncInputEndpoint(toEndpoint, new ShardedShuffleInput(this, shardId, shardingInfo.AllShards.Length, toEndpoint));


            string fromEndpoint = GetEndpointNameForVertex(VertexName.Split('$')[0], _fromToConnections);
            /*var toTuple = _fromToConnections[new Tuple<string, string>(VertexName.Split('$')[0], fromEndpoint)];
            if (toTuple.Item3)
                throw new NotImplementedException("Shared memory endpoints are not supported yet!!");
            else*/
            AddAsyncOutputEndpoint(fromEndpoint, new ShardedShuffleOutput(this, shardId, shardingInfo.AllShards.Length, fromEndpoint));
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
