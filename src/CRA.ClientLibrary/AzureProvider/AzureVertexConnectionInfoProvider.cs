namespace CRA.ClientLibrary.AzureProvider
{
    using CRA.ClientLibrary.DataProvider;
    using Microsoft.WindowsAzure.Storage.Table;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;

    public class AzureVertexConnectionInfoProvider
        : IVertexConnectionInfoProvider
    {
        private CloudTable _cloudTable;

        public AzureVertexConnectionInfoProvider(CloudTable cloudTable)
        { _cloudTable = cloudTable; }

        public Task Add(VertexConnectionInfo vertexConnectionInfo)
            => _cloudTable.ExecuteAsync(
                TableOperation.InsertOrReplace(
                    (ConnectionTable)vertexConnectionInfo));

        public async Task<bool> ContainsRow(VertexConnectionInfo entity)
        {
            var temp = await GetAll();

            return temp
                .Where(gn => entity.Equals(gn))
                .Count() > 0;
        }

        public async Task<int> CountAll()
        {
            var query = new TableQuery<ConnectionTable>();
            return (await _cloudTable.ExecuteQueryAsync(query)).Count();
        }

        public Task Delete(VertexConnectionInfo vci)
            => _cloudTable.ExecuteAsync(
                TableOperation.Delete((ConnectionTable)vci));

        public Task DeleteStore()
            => _cloudTable.DeleteIfExistsAsync();

        public async Task<VertexConnectionInfo?> Get(string fromVertex, string fromOutput, string toConnection, string toInput)
        {
            var connectionTable = new ConnectionTable(
                fromVertex,
                fromOutput,
                toConnection,
                toInput);

            TableOperation retrieveOperation = TableOperation.Retrieve<ConnectionTable>(
                connectionTable.PartitionKey,
                connectionTable.RowKey);

            TableResult retrievedResult = await _cloudTable.ExecuteAsync(retrieveOperation);
            if (retrievedResult.Result == null)
            { return null; }

            var result = (ConnectionTable)retrievedResult.Result;
            return new VertexConnectionInfo(result.FromVertex, result.FromEndpoint, result.ToVertex, result.ToEndpoint, result.ETag);

            //return (VertexConnectionInfo)retrievedResult.Result;
        }

        public async Task<IEnumerable<VertexConnectionInfo>> GetAll()
        {
            var query = new TableQuery<ConnectionTable>();
            return (await _cloudTable.ExecuteQueryAsync(query))
                .Select(_ => (VertexConnectionInfo)_);
        }

        public async Task<IEnumerable<VertexConnectionInfo>> GetAllConnectionsFromVertex(string fromVertex)
        {
            return (await GetAll())
                .Where(gn => fromVertex == gn.FromVertex);
        }

        public async Task<IEnumerable<VertexConnectionInfo>> GetAllConnectionsToVertex(string toVertex)
        {
            return (await GetAll())
                .Where(gn => toVertex == gn.ToVertex);
        }
    }
}
