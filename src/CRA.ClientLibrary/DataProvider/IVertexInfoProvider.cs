namespace CRA.DataProvider
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
        Task<VertexInfo?> GetInstanceFromAddress(string address, int port);
        Task<VertexInfo?> GetRowForInstance(string instanceName);
        Task<IEnumerable<VertexInfo>> GetAllRowsForInstance(string instanceName);
        Task<VertexInfo?> GetRowForInstanceVertex(string instanceName, string vertexName);
        Task DeleteStore();
        Task<VertexInfo?> GetRowForVertexDefinition(string vertexDefinition);
        Task<VertexInfo?> GetRowForActiveVertex(string vertexName);
        Task<IEnumerable<VertexInfo>> GetVertices(string instanceName);
        Task<IEnumerable<VertexInfo>> GetRowsForShardedInstanceVertex(
            string instanceName,
            string vertexName);

        Task<IEnumerable<VertexInfo>> GetRowsForShardedVertex(string vertexName);
        Task<IEnumerable<VertexInfo>> GetRowsForVertex(string vertexName);
        Task<bool> ContainsRow(VertexInfo entity);
        Task<bool> ContainsInstance(string instanceName);
        Task<IEnumerable<string>> GetVertexNames();
        Task<IEnumerable<string>> GetVertexDefinitions();
        Task<IEnumerable<string>> GetInstanceNames();
        Task DeleteVertexInfo(string instanceName, string vertexName);
        Task DeleteVertexInfo(VertexInfo vertexInfo);
        Task InsertOrReplace(VertexInfo newInfo);
    }
}
