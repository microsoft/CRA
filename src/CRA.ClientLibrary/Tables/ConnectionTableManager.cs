using System.Collections.Generic;
using System.Linq;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System.Diagnostics;

namespace CRA.ClientLibrary
{
    /// <summary>
    /// An assignment of one machine to a group
    /// </summary>
    public class ConnectionTableManager
    {
        private CloudTable _connectionTable;

        internal ConnectionTableManager(string storageConnectionString)
        {
            var _storageAccount = CloudStorageAccount.Parse(storageConnectionString);
            var _tableClient = _storageAccount.CreateCloudTableClient();
            _connectionTable = CreateTableIfNotExists("craconnectiontable", _tableClient);
        }

        internal void DeleteTable()
        {
            _connectionTable.DeleteIfExistsAsync().Wait();
        }


        internal void AddConnection(string fromVertex, string fromOutput, string toConnection, string toInput)
        {
            // Make the connection information stable
            var newRow = new ConnectionTable(fromVertex, fromOutput, toConnection, toInput);
            TableOperation insertOperation = TableOperation.InsertOrReplace(newRow);
            _connectionTable.ExecuteAsync(insertOperation).Wait();
        }

        internal void DeleteConnection(string fromVertex, string fromOutput, string toConnection, string toInput)
        {
            // Make the connection information stable
            var newRow = new ConnectionTable(fromVertex, fromOutput, toConnection, toInput);
            newRow.ETag = "*";
            TableOperation retrieveOperation = TableOperation.Retrieve<ConnectionTable>(newRow.PartitionKey, newRow.RowKey);
            TableResult retrievedResult = _connectionTable.ExecuteAsync(retrieveOperation).GetAwaiter().GetResult();
            ConnectionTable deleteEntity = (ConnectionTable)retrievedResult.Result;
            if (deleteEntity != null)
            {
                TableOperation deleteOperation = TableOperation.Delete(deleteEntity);
                _connectionTable.ExecuteAsync(deleteOperation).Wait();
            }
        }

        internal List<ConnectionInfo> GetConnectionsFromVertex(string vertexName)
        {
            TableQuery<ConnectionTable> query = new TableQuery<ConnectionTable>()
                .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, vertexName));

            return _connectionTable
                .ExecuteQuery(query)
                .Select(e => new ConnectionInfo(e.FromVertex, e.FromEndpoint, e.ToVertex, e.ToEndpoint))
                .ToList();
        }

        internal List<ConnectionInfo> GetConnectionsToVertex(string vertexName)
        {
            return
                ConnectionTable.GetAllConnectionsToVertex(_connectionTable, vertexName)
                    .Select(e => new ConnectionInfo(e.FromVertex, e.FromEndpoint, e.ToVertex, e.ToEndpoint))
                    .ToList();
        }

        private CloudTable CreateTableIfNotExists(string tableName, CloudTableClient _tableClient)
        {
            CloudTable table = _tableClient.GetTableReference(tableName);
            try
            {
                Debug.WriteLine("Creating table " + tableName);
                table.CreateIfNotExistsAsync().Wait();
            }
            catch { }

            return table;
        }
    }
}
