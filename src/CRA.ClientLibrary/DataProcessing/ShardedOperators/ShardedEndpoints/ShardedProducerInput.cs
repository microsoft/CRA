using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace CRA.ClientLibrary.DataProcessing
{
    public class ShardedProducerInput : ShardedOperatorInputBase
    {
        private ShardedProducerOperator _vertex;

        public ShardedProducerInput(IVertex vertex, int shardId, int numOtherOperatorShards, string endpointName) : base(shardId, numOtherOperatorShards, endpointName)
        {
            _vertex = (ShardedProducerOperator)vertex;
        }

        public override async Task OperatorInputFromStreamAsync(Stream stream, string otherVertex, int otherShardId, string otherEndpoint, CancellationToken token)
        {
            _startReceivingFromOtherOperatorShards.Signal();
            _startReceivingFromOtherOperatorShards.Wait();

            throw new NotImplementedException();

            _finishReceivingFromOtherOperatorShards.Signal();
            _finishReceivingFromOtherOperatorShards.Wait();
        }
    }
}
