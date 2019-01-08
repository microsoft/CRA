//-----------------------------------------------------------------------
// <copyright file="IVertexInfoProvider.cs" company="">
//     Copyright (c) . All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

namespace CRA.ClientLibrary.DataProvider
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    /// <summary>
    /// Definition for IVertexInfoProvider
    /// </summary>
    public interface IVertexInfoProvider
    {
        Task<IEnumerable<VertexInfo>> GetAll();
        Task<int> CountAll();
        Task<VertexInfo> GetInstanceFromAddress(string address, int port);
        Task<VertexInfo> GetRowForInstance(string instanceName);
        Task<IEnumerable<VertexInfo>> GetAllRowsForInstance(string instanceName);
        Task<VertexInfo> GetRowForInstanceVertex(string instanceName, string vertexName);
        Task DeleteStore();
        Task<VertexInfo> GetRowForVertexDefinition(string vertexDefinition);
        Task<VertexInfo> GetRowForVertex(string vertexName);
        Task<IEnumerable<VertexInfo>> GetVertices(string instanceName);
        Task<IEnumerable<VertexInfo>> GetRowsForShardedVertex(string vertexName);
        Task<IEnumerable<VertexInfo>> GetRowsForVertex(string vertexName);
        Task<bool> ContainsRow(VertexInfo entity);
        Task<bool> ContainsInstance(string instanceName);
        Task<List<string>> GetVertexNames();
        Task<List<string>> GetVertexDefinitions();
        Task<List<string>> GetInstanceNames();
        Task RegisterVertexInfo(VertexInfo vertexInfo);
        Task UpdateVertex(VertexInfo newActiveVertex);
        Task DeleteVertexInfo(string instanceName, string vertexName);
    }
}
