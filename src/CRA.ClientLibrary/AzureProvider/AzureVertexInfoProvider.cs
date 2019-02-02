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

        public Task<VertexInfo?> GetInstanceFromAddress(string address, int port)
            => Get(gn => address == gn.Address && port == gn.Port);

        public Task<VertexInfo?> GetRowForInstance(string instanceName)
            => Get(gn => instanceName == gn.InstanceName && string.IsNullOrEmpty(gn.VertexName));

        public async Task<IEnumerable<VertexInfo>> GetAllRowsForInstance(string instanceName)
            => (await GetAll()).Where(gn => instanceName == gn.InstanceName);

        public Task<VertexInfo?> GetRowForInstanceVertex(string instanceName, string vertexName)
            => Get(gn => instanceName == gn.InstanceName && vertexName == gn.VertexName);

        public Task<VertexInfo?> GetRowForVertexDefinition(string vertexDefinition)
            => Get(gn => vertexDefinition == gn.VertexName && string.IsNullOrEmpty(gn.InstanceName));

        public Task<VertexInfo?> GetRowForActiveVertex(string vertexName)
            => Get(gn => vertexName == gn.VertexName
                && !string.IsNullOrEmpty(gn.InstanceName)
                && gn.IsActive);

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

        public async Task<IEnumerable<string>> GetVertexNames()
            => (await cloudTable.ExecuteQueryAsync(
                new TableQuery<VertexTable>()
                 .Where(
                    TableQuery.GenerateFilterCondition(
                        "PartitionKey",
                        QueryComparisons.NotEqual,
                        ""))))
                .Select(e => e.VertexName)
                .ToList();

        public async Task<IEnumerable<string>> GetVertexDefinitions()
            => (await cloudTable.ExecuteQueryAsync(
                new TableQuery<VertexTable>()
                 .Where(
                    TableQuery.GenerateFilterCondition(
                        "PartitionKey",
                        QueryComparisons.Equal,
                        ""))))
                .Select(e => e.VertexDefinition)
                .ToList();

        public async Task<IEnumerable<string>> GetInstanceNames()
            => (await cloudTable.ExecuteQueryAsync(
                new TableQuery<VertexTable>()
                    .Where(
                        TableQuery.GenerateFilterCondition(
                            "RowKey",
                            QueryComparisons.Equal,
                            ""))))
                .Select(e => e.InstanceName)
                .ToList();

        public Task DeleteVertexInfo(string instanceName, string vertexName)
        {
            var newRow = new DynamicTableEntity(instanceName, "");
            newRow.ETag = "*";
            TableOperation deleteOperation = TableOperation.Delete(newRow);
            return cloudTable.ExecuteAsync(deleteOperation);
        }

        public Task DeleteVertexInfo(VertexInfo vertexInfo)
            => cloudTable.ExecuteAsync(
                TableOperation.Delete((VertexTable)vertexInfo));

        public Task InsertOrReplace(VertexInfo newActiveVertex)
            => cloudTable.ExecuteAsync(
                TableOperation.InsertOrReplace(
                    (VertexTable)newActiveVertex));

        public async Task<IEnumerable<VertexInfo>> GetRowsForShardedInstanceVertex(string instanceName, string vertexName)
            => (await cloudTable.ExecuteQueryAsync(
                new TableQuery<VertexTable>()
                 .Where(
                    TableQuery.GenerateFilterCondition(
                        "PartitionKey",
                        QueryComparisons.Equal,
                        instanceName))))
                .Where(e => e.VertexName.StartsWith(vertexName + "$"))
                .Select(e => (VertexInfo)e)
                .ToList();

        private async Task<VertexInfo?> Get(System.Func<VertexInfo, bool> filter)
        {
            var item = (await GetAll()).Where(filter).FirstOrDefault();
            if (item == default(VertexInfo))
            { return null; }

            return item;
        }
    }
}
