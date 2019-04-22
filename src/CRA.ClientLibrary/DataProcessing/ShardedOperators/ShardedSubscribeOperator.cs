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
    public class ShardedSubscribeOperator : ShardedOperatorBase
    {
        internal CountdownEvent _deploySubscribeInput;
        internal CountdownEvent _deploySubscribeOutput;

        internal CountdownEvent _runSubscribeInput;
        internal CountdownEvent _runSubscribeOutput;

        internal string _outputObserver;

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

            string toEndpoint = GetEndpointNameForVertex(VertexName.Split('$')[0], _toFromConnections);
            /*var fromTuple = _toFromConnections[new Tuple<string, string>(VertexName.Split('$')[0], toEndpoint)];
            if (fromTuple.Item3)
                throw new NotImplementedException("Shared memory endpoints are not supported yet!!");
            else*/
                AddAsyncInputEndpoint(toEndpoint, new ShardedSubscribeInput(this, shardId, shardingInfo.AllShards.Length, toEndpoint));

            string fromEndpoint = GetEndpointNameForVertex(VertexName.Split('$')[0], _fromToConnections);
            AddAsyncOutputEndpoint(fromEndpoint, new ShardedSubscribeOutput(this, shardId, shardingInfo.AllShards.Length, fromEndpoint));
        }

        internal override bool HasSplittedOutput()
        {
            return false;
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
    }
}
