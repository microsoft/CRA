using System;
using System.Collections.Generic;
using System.IO;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CRA.ClientLibrary.DataProcessing
{
    public abstract class ShardedOperatorOutputBase : AsyncShardedVertexOutputEndpointBase
    {
        protected int _shardId;
        protected string _endpointName;

        protected byte[] _deployMsgBuffer;
        protected byte[] _runMsgBuffer;
        protected CountdownEvent _startSendingToOtherOperatorShards;
        protected CountdownEvent _finishSendingToOtherOperatorShards;

        public ShardedOperatorOutputBase(int shardId, int numOtherOperatorShards, string endpointName)
        {
            _shardId = shardId;
            _endpointName = endpointName;

            _deployMsgBuffer = new byte[Encoding.ASCII.GetBytes("DEPLOY").Length];
            _runMsgBuffer = new byte[Encoding.ASCII.GetBytes("RUN").Length];

            _startSendingToOtherOperatorShards = new CountdownEvent(numOtherOperatorShards);
            _finishSendingToOtherOperatorShards = new CountdownEvent(numOtherOperatorShards);
        }

        public override void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                Console.WriteLine("Disposing " + _endpointName);
            }
        }

        public override async Task ToStreamAsync(Stream stream, string otherVertex, int otherShardId, string otherEndpoint, CancellationToken token) 
                => await OperatorOutputToStreamAsync(stream, otherVertex, otherShardId, otherEndpoint, token);

        public abstract Task OperatorOutputToStreamAsync(Stream stream, string otherVertex, int otherShardId, string otherEndpoint, CancellationToken token);

        public void StartProducer<TKey, TPayload, TDataset>(Object dataset, Stream stream)
            where TDataset : IDataset<TKey, TPayload>
        {
            ((TDataset)dataset).ToStream(stream);
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
