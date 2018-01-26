using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace CRA.ClientLibrary
{
    /// <summary>
    /// Interface for async output endpoints in CRA
    /// </summary>
    public interface IAsyncVertexOutputEndpoint : IDisposable
    {
        /// <summary>
        /// Async version of ToStream
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="otherVertex"></param>
        /// <param name="otherEndpoint"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        Task ToStreamAsync(Stream stream, string otherVertex, string otherEndpoint, CancellationToken token);
    }

    /// <summary>
    /// Interface for async output endpoints in CRA
    /// </summary>
    public interface IAsyncShardedVertexOutputEndpoint : IAsyncVertexOutputEndpoint
    {
        /// <summary>
        /// Async version of ToStream
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="otherVertex"></param>
        /// <param name="otherEndpoint"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        Task ToStreamAsync(Stream stream, string otherVertex, int otherShardId, string otherEndpoint, CancellationToken token);

        /// <summary>
        /// Provide info on sharding for the "other" sharded vertex that 
        /// this endpoint writes to.
        /// </summary>
        /// <param name="otherVertex"></param>
        /// <param name="shardingInfo"></param>
        void UpdateShardingInfo(string otherVertex, ShardingInfo shardingInfo);
    }

    /// <summary>
    /// Interface for async output endpoints in CRA with fusable output
    /// </summary>
    public interface IAsyncFusableVertexOutputEndpoint : IAsyncVertexOutputEndpoint
    {
        /// <summary>
        /// Can this output endpoint fuse with the specified input endpoint?
        /// </summary>
        /// <param name="endpoint"></param>
        /// <param name="otherVertex"></param>
        /// <param name="otherEndpoint"></param>
        /// <returns></returns>
        bool CanFuseWith(IAsyncVertexInputEndpoint endpoint, string otherVertex, string otherEndpoint);

        /// <summary>
        /// Async version of ToInput
        /// </summary>
        /// <param name="endpoint"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        Task ToInputAsync(IAsyncVertexInputEndpoint endpoint, string otherVertex, string otherEndpoint, CancellationToken token);
    }
}
