using CRA.ClientLibrary;
using System;
using System.Threading.Tasks;

namespace ShardedConnectionPair
{
    public class ShardedConnectionPairVertex : ShardedVertexBase
    {
        public ShardedConnectionPairVertex() : base()
        {
        }

        public override Task InitializeAsync(int shardId, ShardingInfo shardingInfo, object vertexParameter)
        {
            Console.WriteLine("Sharded vertex name: {0}", GetVertexName());

            AddAsyncInputEndpoint("input", new MyAsyncInput(this));
            AddAsyncOutputEndpoint("output", new MyAsyncOutput(this));

            return Task.CompletedTask;
        }
    }
}
