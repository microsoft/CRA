//-----------------------------------------------------------------------
// <copyright file="AzureVertexInfoProvider.cs" company="">
//     Copyright (c) . All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

namespace CRA.ClientLibrary.AzureProvider
{
    using CRA.ClientLibrary.DataProvider;
    using Microsoft.WindowsAzure.Storage.Table;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;

    /// <summary>
    /// Definition for AzureVertexInfoProvider
    /// </summary>
    public class AzureVertexInfoProvider : IVertexInfoProvider
    {
        private readonly CloudTable cloudTable;

        public AzureVertexInfoProvider(CloudTable cloudTable)
        {
            this.cloudTable = cloudTable;
        }

        public async Task<IEnumerable<VertexInfo>> GetAll()
            => (await cloudTable.ExecuteQueryAsync(new TableQuery<VertexTable>()))
                .Select(vt => (VertexInfo)vt);

        /// <summary>
        /// Counts all nodes in the cluster regardless of their group
        /// </summary>
        /// <returns></returns>
        public async Task<int> CountAll()
            => (await GetAll()).Count();

        public async Task<VertexInfo> GetInstanceFromAddress(string address, int port)
            => (await GetAll()).Where(gn => address == gn.Address && port == gn.Port).First();

        public async Task<VertexInfo> GetRowForInstance(string instanceName)
            => (await GetAll()).Where(gn => instanceName == gn.InstanceName && string.IsNullOrEmpty(gn.VertexName)).First();
        public async Task<IEnumerable<VertexInfo>> GetAllRowsForInstance(string instanceName)
            => (await GetAll()).Where(gn => instanceName == gn.InstanceName);

        public async Task<VertexInfo> GetRowForInstanceVertex(string instanceName, string vertexName)
            => (await GetAll()).Where(gn => instanceName == gn.InstanceName && vertexName == gn.VertexName).First();

        public async Task<VertexInfo> GetRowForVertexDefinition(string vertexDefinition)
            => (await GetAll()).Where(gn => vertexDefinition == gn.VertexName && string.IsNullOrEmpty(gn.InstanceName)).First();

        public async Task<VertexInfo> GetRowForVertex(string vertexName)
            => (await GetAll())
                .Where(gn => vertexName == gn.VertexName && !string.IsNullOrEmpty(gn.InstanceName))
                .Where(gn => gn.IsActive)
                .First();

        public async Task<IEnumerable<VertexInfo>> GetVertices(string instanceName)
            => (await GetAll()).Where(gn => instanceName == gn.InstanceName && !string.IsNullOrEmpty(gn.VertexName));

        public async Task<IEnumerable<VertexInfo>> GetRowsForShardedVertex(string vertexName)
            => (await GetAll()).Where(gn => gn.VertexName.StartsWith(vertexName + "$") && !string.IsNullOrEmpty(gn.VertexName));

        public async Task<bool> ContainsRow(VertexInfo entity)
            => (await GetAll()).Where(gn => entity.Equals(gn)).Count() > 0;

        public async Task<bool> ContainsInstance(string instanceName)
            => (await GetAll()).Where(gn => instanceName == gn.InstanceName).Count() > 0;
}
}
