using CRA.ClientLibrary.DataProvider;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CRA.ClientLibrary.DataProcessing
{
    public class ShardedSubscribeClientOperator : ShardedOperatorBase
    {
        internal CountdownEvent _deploySubscribeInput;
        internal CountdownEvent _deploySubscribeOutput;

        internal CountdownEvent _runSubscribeInput;
        internal CountdownEvent _runSubscribeOutput;

        internal string _outputObserver;

        internal int _numShardsConnectingTo;

        public ShardedSubscribeClientOperator() : base()
        {
        }

        internal override void InitializeOperator(int shardId, ShardingInfo shardingInfo)
        {
            _hasSplittedOutput = HasSplittedOutput();

            _numShardsConnectingTo = 0;
            var instancesMap = _task.DeployDescriptor.InstancesMap();
            foreach (var entry in instancesMap.Keys)
                _numShardsConnectingTo += instancesMap[entry];
 
            _deploySubscribeInput = new CountdownEvent(1);
            _deploySubscribeOutput = new CountdownEvent(_numShardsConnectingTo);

            _runSubscribeInput = new CountdownEvent(1);
            _runSubscribeOutput = new CountdownEvent(_numShardsConnectingTo);

            string[] toEndpoints = GetEndpointNamesForVertex(VertexName.Split('$')[0], _toFromConnections);
            var fromTuple = _toFromConnections[new Tuple<string, string>(VertexName.Split('$')[0], toEndpoints[0])];
            if (!fromTuple.Item4)
                AddAsyncInputEndpoint(toEndpoints[0], new ShardedSubscribeClientInput(this, shardId, shardingInfo.AllShards.Length, toEndpoints[0]));
            else
                throw new NotImplementedException("Shared secondary endpoints are not supported in subscribe operators!!");

            string fromEndpoint = "OutputToClient" + Guid.NewGuid().ToString();
            AddAsyncOutputEndpoint(fromEndpoint, new ShardedSubscribeClientOutput(this, shardId, shardingInfo.AllShards.Length, fromEndpoint));
        }

        internal override bool HasSplittedOutput()
        {
            return false;
        }
    }
}