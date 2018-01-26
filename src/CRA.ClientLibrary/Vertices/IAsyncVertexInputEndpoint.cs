using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace CRA.ClientLibrary
{
    /// <summary>
    /// Interface for async input endpoints in CRA
    /// </summary>
    public interface IAsyncVertexInputEndpoint : IDisposable
    {
        /// <summary>
        /// Async version of FromStream
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="otherVertex"></param>
        /// <param name="otherEndpoint"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        Task FromStreamAsync(Stream stream, string otherVertex, string otherEndpoint, CancellationToken token);
    }

    /// <summary>
    /// Interface for async sharded input endpoints in CRA
    /// </summary>
    public interface IAsyncShardedVertexInputEndpoint : IAsyncVertexInputEndpoint
    {
        /// <summary>
        /// Async version of FromStream
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="otherVertex"></param>
        /// <param name="otherEndpoint"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        Task FromStreamAsync(Stream stream, string otherVertex, int otherShardId, string otherEndpoint, CancellationToken token);

        /// <summary>
        /// Provide info on sharding for the "other" sharded vertex that 
        /// this endpoint reads from.
        /// </summary>
        /// <param name="otherVertex"></param>
        /// <param name="shardingInfo"></param>
        void UpdateShardingInfo(string otherVertex, ShardingInfo shardingInfo);
    }
}
