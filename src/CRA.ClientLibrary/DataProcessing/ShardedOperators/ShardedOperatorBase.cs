using CRA.ClientLibrary.DataProvider;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace CRA.ClientLibrary.DataProcessing
{
    public abstract class ShardedOperatorBase : ShardedVertexBase
    {
        internal TaskBase _task;

        internal Dictionary<int, Dictionary<string, object>> _cachedDatasets;

        protected ShardingInfo _shardingInfo;
        protected ConcurrentDictionary<Tuple<string, string>, Tuple<string, string, bool>> _fromToConnections;
        protected ConcurrentDictionary<Tuple<string, string>, Tuple<string, string, bool>> _toFromConnections;

        internal bool _hasSecondaryInput = false;
        internal bool _hasSplittedOutput = false;

        public ShardedOperatorBase() : base()
        {
            _cachedDatasets = new Dictionary<int, Dictionary<string, object>>();
        }

        public override void Initialize(int shardId, ShardingInfo shardingInfo, object vertexParameter)
        {
            _shardingInfo = shardingInfo;   

            PrepareOperatorParameter(vertexParameter); 
            PrepareAllConnectionsMap();

            _cachedDatasets[shardId] = new Dictionary<string, object>();

            InitializeOperator(shardId, shardingInfo);
        }

        private void PrepareOperatorParameter(object vertexParameter)
        {
            if (vertexParameter is ProduceTask)
                _task = (ProduceTask)vertexParameter;
            else if (vertexParameter is SubscribeTask)
                _task = (SubscribeTask)vertexParameter;
            else if (vertexParameter is ShuffleTask)
                _task = (ShuffleTask)vertexParameter;
            else if (vertexParameter is ClientTerminalTask)
                _task = (ClientTerminalTask)vertexParameter;
            else if (vertexParameter is TaskBase)
                _task = (TaskBase)vertexParameter;
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

        internal abstract void InitializeOperator(int shardId, ShardingInfo shardingInfo);

        internal string GetEndpointNameForVertex(string vertexName, ConcurrentDictionary<Tuple<string, string>, Tuple<string, string, bool>> connectionsMap)
        {
            string endpointName = "";
            foreach (var key in connectionsMap.Keys)
            {
                if (key.Item1 == vertexName)
                {
                    endpointName = key.Item2;
                    break;
                }

            }
            return endpointName;
        }

        internal abstract bool HasSplittedOutput();
    }
}