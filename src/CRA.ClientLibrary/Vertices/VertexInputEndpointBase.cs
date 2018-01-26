using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace CRA.ClientLibrary
{
    /// <summary>
    /// Base class for Vertex abstraction
    /// </summary>
    public abstract class AsyncShardedVertexInputEndpointBase : IAsyncShardedVertexInputEndpoint
    {
        public abstract void Dispose();
        public virtual Task FromStreamAsync(Stream stream, string otherVertex, int otherShardId, string otherEndpoint, CancellationToken token)
        {
            return FromStreamAsync(stream, otherVertex + "$" + otherShardId, otherEndpoint, token);
        }

        public virtual Task FromStreamAsync(Stream stream, string otherVertex, string otherEndpoint, CancellationToken token)
        {
            return Task.CompletedTask;
        }

        public virtual void UpdateShardingInfo(string otherVertex, ShardingInfo shardingInfo)
        {
        }
    }
}
