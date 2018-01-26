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
    public abstract class AsyncShardedVertexOutputEndpointBase : IAsyncShardedVertexOutputEndpoint
    {
        public abstract void Dispose();
        public virtual Task ToStreamAsync(Stream stream, string otherVertex, int otherShardId, string otherEndpoint, CancellationToken token)
        {
            return ToStreamAsync(stream, otherVertex + "$" + otherShardId, otherEndpoint, token);
        }
        public virtual Task ToStreamAsync(Stream stream, string otherVertex, string otherEndpoint, CancellationToken token)
        {
            return Task.CompletedTask;
        }
    }
}
