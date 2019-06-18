namespace CRA.DataProvider
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    /// <summary>
    /// Definition for IVertexConnectionInfoProvider
    /// </summary>
    public interface IVertexConnectionInfoProvider
    {
        Task<IEnumerable<VertexConnectionInfo>> GetAll();

        Task<int> CountAll();

        Task<IEnumerable<VertexConnectionInfo>> GetAllConnectionsFromVertex(string fromVertex);
        Task DeleteStore();
        Task<IEnumerable<VertexConnectionInfo>> GetAllConnectionsToVertex(string toVertex);
        Task Add(VertexConnectionInfo vertexConnectionInfo);
        Task<bool> ContainsRow(VertexConnectionInfo entity);
        Task<VertexConnectionInfo?> Get(string fromVertex, string fromOutput, string toConnection, string toInput);
        Task Delete(VertexConnectionInfo vci);
    }
}
