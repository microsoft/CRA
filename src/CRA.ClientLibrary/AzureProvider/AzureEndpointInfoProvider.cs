namespace CRA.ClientLibrary.AzureProvider
{
    using CRA.ClientLibrary.DataProvider;
    using Microsoft.WindowsAzure.Storage.Table;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;

    /// <summary>
    /// Definition for AzureEndpointInfoProvider
    /// </summary>
    public class AzureEndpointInfoProvider
        : IEndpointInfoProvider
    {
        private readonly CloudTable _cloudTable;

        public AzureEndpointInfoProvider(CloudTable cloudTable)
        { _cloudTable = cloudTable; }

        public Task AddEndpoint(EndpointInfo endpointInfo)
            => _cloudTable.ExecuteAsync(
                TableOperation.InsertOrReplace((EndpointTable)endpointInfo));

        public Task DeleteEndpoint(string vertexName, string endpointName, string versionId)
            => _cloudTable.ExecuteAsync(
                TableOperation.Delete(
                    new DynamicTableEntity(vertexName, endpointName)
                    { ETag = versionId }));

        public Task DeleteEndpoint(EndpointInfo endpointInfo)
            => DeleteEndpoint(
                endpointInfo.VertexName,
                endpointInfo.EndpointName,
                endpointInfo.VersionId);

        public Task DeleteStore()
            => _cloudTable.DeleteIfExistsAsync();

        public async Task<bool> ExistsEndpoint(string vertexName, string endPoint)
            => (await _cloudTable.ExecuteQueryAsync(
                new TableQuery<EndpointTable>()
                    .Where(TableQuery.CombineFilters(
                        TableQuery.GenerateFilterCondition(
                            "PartitionKey",
                            QueryComparisons.Equal,
                            vertexName),
                        TableOperators.And,
                        TableQuery.GenerateFilterCondition(
                            "RowKey",
                            QueryComparisons.Equal,
                            endPoint)))))
                .Any();

        public async Task<IEnumerable<EndpointInfo>> GetAll()
            => (await _cloudTable.ExecuteQueryAsync(new TableQuery<EndpointTable>()))
                           .Select(et => (EndpointInfo)et)
                           .ToList();

        public async Task<EndpointInfo?> GetEndpoint(string vertexName, string endpointName)
        {
            var et = (await _cloudTable.ExecuteAsync(
                TableOperation.Retrieve<EndpointTable>(
                    vertexName,
                    endpointName))).Result;

            if (et == null)
            { return null; }

            return (EndpointInfo)et;
        }

        public async Task<List<EndpointInfo>> GetEndpoints(string vertexName)
            => (await _cloudTable.ExecuteQueryAsync(
                new TableQuery<EndpointTable>()
                    .Where(
                    TableQuery.GenerateFilterCondition(
                        "PartitionKey",
                        QueryComparisons.Equal,
                        vertexName))))
            .Select(et => (EndpointInfo)et)
            .ToList();

        public async Task<List<EndpointInfo>> GetShardedEndpoints(string vertexName, string endpointName)
            => (await _cloudTable.ExecuteQueryAsync(
                new TableQuery<EndpointTable>()
                 .Where(
                    TableQuery.GenerateFilterCondition(
                        "RowKey",
                        QueryComparisons.Equal,
                        endpointName))))
                .Where(e => e.VertexName.StartsWith(vertexName + "$"))
                .Select(e => (EndpointInfo)e)
                .ToList();
    }
}
