using System;
using System.Collections.Concurrent;
using System.Threading.Tasks;

namespace CRA.ClientLibrary
{

    /// <summary>
    /// User provided notion of a running vertex
    /// </summary>
    public interface IVertex : IDisposable
    {
        /// <summary>
        /// Ingress points for a vertex; these are observers
        /// </summary>
        ConcurrentDictionary<string, IVertexInputEndpoint> InputEndpoints { get; }

        /// <summary>
        /// Egress points for a vertex; these are observables
        /// </summary>
        ConcurrentDictionary<string, IVertexOutputEndpoint> OutputEndpoints { get; }

        /// <summary>
        /// Ingress points for a vertex; these are observers
        /// </summary>
        ConcurrentDictionary<string, IAsyncVertexInputEndpoint> AsyncInputEndpoints { get; }

        /// <summary>
        /// Egress points for a vertex; these are observables
        /// </summary>
        ConcurrentDictionary<string, IAsyncVertexOutputEndpoint> AsyncOutputEndpoints { get; }


        /// <summary>
        /// Callback that vertex will invoke when a new input is added
        /// </summary>
        /// <param name="addInputCallback"></param>
        void OnAddInputEndpoint(Action<string, IVertexInputEndpoint> addInputCallback);

        /// <summary>
        /// Callback that vertex will invoke when a new output is added
        /// </summary>
        /// <param name="addOutputCallback"></param>
        void OnAddAsyncOutputEndpoint(Action<string, IAsyncVertexOutputEndpoint> addOutputCallback);

        /// <summary>
        /// Callback that vertex will invoke when a new input is added
        /// </summary>
        /// <param name="addInputCallback"></param>
        void OnAddAsyncInputEndpoint(Action<string, IAsyncVertexInputEndpoint> addInputCallback);

        /// <summary>
        /// Callback that vertex will invoke when a new output is added
        /// </summary>
        /// <param name="addOutputCallback"></param>
        void OnAddOutputEndpoint(Action<string, IVertexOutputEndpoint> addOutputCallback);

        /// <summary>
        /// Callback when vertex is disposed
        /// </summary>
        void OnDispose(Action disposeCallback);

        /// <summary>
        /// Gets an instance of the CRA Client Library that the vertex
        /// can use to communicate with the CRA runtime.
        /// </summary>
        /// <returns>Instance of CRA Client Library</returns>
        CRAClientLibrary ClientLibrary { get; }

        /// <summary>
        /// Initialize vertex asynchronously with specified params
        /// </summary>
        /// <param name="vertexParameter"></param>
        /// <returns></returns>
        Task InitializeAsync(object vertexParameter);
    }

    /// <summary>
    /// User provided notion of a running sharded vertex
    /// </summary>
    public interface IShardedVertex : IVertex
    {
        /// <summary>
        /// Initialize sharded vertex with shard-id and specified params
        /// </summary>
        /// <param name="shardId"></param>
        /// <param name="vertexParameter"></param>
        Task InitializeAsync(int shardId, ShardingInfo shardingInfo, object vertexParameter);

        /// <summary>
        /// Update sharding info
        /// </summary>
        /// <param name="shardingInfo"></param>
        void UpdateShardingInfo(ShardingInfo shardingInfo);
    }
}
