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
        {
            var query = new TableQuery<VertexTable>()
                .Where(
                TableQuery.CombineFilters(
            TableQuery.GenerateFilterCondition("RowKey",
                QueryComparisons.GreaterThanOrEqual,
                vertexName + "$"),
            TableOperators.And,
            TableQuery.GenerateFilterCondition("RowKey",
                QueryComparisons.LessThan,
                vertexName + "$9999999999999")
            ));

            return (await cloudTable.ExecuteQueryAsync(query))
                .Select(e => (VertexInfo)e)
                .ToList();
        }

        public async Task<bool> ContainsRow(VertexInfo entity)
            => (await GetAll()).Where(gn => entity.Equals(gn)).Count() > 0;

        public async Task<bool> ContainsInstance(string instanceName)
            => (await GetAll()).Where(gn => instanceName == gn.InstanceName).Count() > 0;

        public Task DeleteStore()
            => cloudTable.DeleteIfExistsAsync();

        public async Task<IEnumerable<VertexInfo>> GetRowsForVertex(string vertexName)
            => (await cloudTable.ExecuteQueryAsync(
                    new TableQuery<VertexTable>()
                        .Where(
                            TableQuery.GenerateFilterCondition(
                                "RowKey",
                                QueryComparisons.Equal,
                                vertexName))))
            .Select(vt => (VertexInfo)vt);

        public async Task<List<string>> GetVertexNames()
            => (await cloudTable.ExecuteQueryAsync(
                new TableQuery<VertexTable>()
                 .Where(
                    TableQuery.GenerateFilterCondition(
                        "PartitionKey",
                        QueryComparisons.NotEqual,
                        ""))))
                .Select(e => e.VertexName)
                .ToList();

        public async Task<List<string>> GetVertexDefinitions()
            => (await cloudTable.ExecuteQueryAsync(
                new TableQuery<VertexTable>()
                 .Where(
                    TableQuery.GenerateFilterCondition(
                        "PartitionKey",
                        QueryComparisons.Equal,
                        ""))))
                .Select(e => e.VertexName)
                .ToList();

        public async Task<List<string>> GetInstanceNames()
        {
            TableQuery<VertexTable> query = new TableQuery<VertexTable>()
                .Where(TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, ""));

            return (await cloudTable.ExecuteQueryAsync(
                new TableQuery<VertexTable>()
                    .Where(
                        TableQuery.GenerateFilterCondition(
                            "RowKey",
                            QueryComparisons.Equal,
                            ""))))
                .Select(e => e.InstanceName)
                .ToList();
        }

        public Task RegisterVertexInfo(VertexInfo vertexInfo)
             => cloudTable.ExecuteAsync(
                 TableOperation.InsertOrReplace(
                     (VertexTable)vertexInfo));

        public Task UpdateVertex(VertexInfo newActiveVertex)
            => cloudTable.ExecuteAsync(
                TableOperation.InsertOrReplace(
                    (VertexTable)newActiveVertex));

        public Task DeleteVertexInfo(string instanceName, string vertexName)
        {
            var newRow = new DynamicTableEntity(instanceName, "");
            newRow.ETag = "*";
            TableOperation deleteOperation = TableOperation.Delete(newRow);
            return cloudTable.ExecuteAsync(deleteOperation);
        }
    }
}
