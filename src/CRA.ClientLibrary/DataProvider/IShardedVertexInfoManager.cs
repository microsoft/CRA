//-----------------------------------------------------------------------
// <copyright file="IShardedVertexInfoManager.cs" company="">
//     Copyright (c) . All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

namespace CRA.ClientLibrary.DataProvider
{
    using System;
    using System.Collections.Generic;
    using System.Linq.Expressions;
    using System.Threading.Tasks;

    /// <summary>
    /// Definition for IShardedVertexInfoManager
    /// </summary>
    public interface IShardedVertexInfoManager
    {
        Task DeleteStore();

        Task RegisterShardedVertex(
            string vertexName,
            List<string> allInstances,
            List<int> allShards,
            List<int> addedShards,
            List<int> removedShards,
            Expression<Func<int, int>> shardLocator);

        Task<bool> ExistsShardedVertex(string vertexName);

        Task<(List<string> allInstances, List<int> allShards, List<int> removeShards, List<int> addesShards)> GetLatestShardedVertex(
            string vertexName);

        Task<ShardingInfo> GetLatestShardingInfo(string vertexName);

        Task DeleteShardedVertex(string vertexName);
    }
}
