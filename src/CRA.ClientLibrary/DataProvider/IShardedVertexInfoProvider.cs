//-----------------------------------------------------------------------
// <copyright file="IShardedShardedVertexInfoProvider.cs" company="">
//     Copyright (c) . All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

namespace CRA.ClientLibrary.DataProvider
{
    using System.Collections.Generic;
    using System.Threading.Tasks;

    /// <summary>
    /// Definition for IShardedShardedVertexInfoProvider
    /// </summary>
    public interface IShardedVertexInfoProvider
    {
        Task<IEnumerable<ShardedVertexInfo>> GetAll();
        Task<int> CountAll();
        Task<ShardedVertexInfo> GetEntryForVertex(string vertexName, string epochId);
        Task<IEnumerable<ShardedVertexInfo>> GetEntriesForVertex(string vertexName);
        Task<ShardedVertexInfo> GetLatestEntryForVertex(string vertexName);
        Task Delete();
        Task Delete(ShardedVertexInfo entry);
        Task Insert(ShardedVertexInfo shardedVertexInfo);
    }
}
