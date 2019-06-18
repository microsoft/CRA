using CRA.DataProvider;
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
    public class ShardedSubscribeOperator : ShardedOperatorBase
    {
        internal CountdownEvent _deploySubscribeInput;
        internal CountdownEvent _deploySubscribeOutput;

        internal CountdownEvent _runSubscribeInput;
        internal CountdownEvent _runSubscribeOutput;

        public ShardedSubscribeOperator() : base()
        {
        }

        internal override void InitializeOperator(int shardId, ShardingInfo shardingInfo)
        {
            _hasSplittedOutput = HasSplittedOutput();

            _deploySubscribeInput = new CountdownEvent(1);
            _deploySubscribeOutput = new CountdownEvent(1);

            _runSubscribeInput = new CountdownEvent(1);
            _runSubscribeOutput = new CountdownEvent(1);

            string[] toEndpoints = GetEndpointNamesForVertex(VertexName.Split('$')[0], _toFromConnections);
            var fromTuple = _toFromConnections[new Tuple<string, string>(VertexName.Split('$')[0], toEndpoints[0])];
            if (!fromTuple.Item4)
                AddAsyncInputEndpoint(toEndpoints[0], new ShardedSubscribeInput(this, shardId, shardingInfo.AllShards.Length, toEndpoints[0]));
            else
                throw new NotImplementedException("Shared secondary endpoints are not supported in subscribe operators!!");

            string[] fromEndpoints = GetEndpointNamesForVertex(VertexName.Split('$')[0], _fromToConnections);
            var toTuple = _fromToConnections[new Tuple<string, string>(VertexName.Split('$')[0], fromEndpoints[0])];
            if (!toTuple.Item4)
                AddAsyncOutputEndpoint(fromEndpoints[0], new ShardedSubscribeOutput(this, shardId, shardingInfo.AllShards.Length, fromEndpoints[0]));
            else
                throw new NotImplementedException("Shared secondary endpoints are not supported in subscribe operators!!");
        }

        internal override bool HasSplittedOutput()
        {
            return false;
        }
    }
}
