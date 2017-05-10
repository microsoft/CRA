using System.Collections.Generic;
using System.Linq;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

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
            _connectionTable = CreateTableIfNotExists("connectiontableforcra", _tableClient);
        }

        internal void DeleteTable()
        {
            _connectionTable.DeleteIfExists();
        }


        internal void AddConnection(string fromProcess, string fromOutput, string toConnection, string toInput)
        {
            // Make the connection information stable
            var newRow = new ConnectionTable(fromProcess, fromOutput, toConnection, toInput);
            TableOperation insertOperation = TableOperation.InsertOrReplace(newRow);
            _connectionTable.Execute(insertOperation);
        }

        internal void DeleteConnection(string fromProcess, string fromOutput, string toConnection, string toInput)
        {
            // Make the connection information stable
            var newRow = new ConnectionTable(fromProcess, fromOutput, toConnection, toInput);
            newRow.ETag = "*";
            TableOperation deleteOperation = TableOperation.Delete(newRow);
            _connectionTable.Execute(deleteOperation);
        }

        internal List<ConnectionInfo> GetConnectionsFromProcess(string processName)
        {
            TableQuery<ConnectionTable> query = new TableQuery<ConnectionTable>()
                .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, processName));

            return _connectionTable
                .ExecuteQuery(query)
                .Select(e => new ConnectionInfo(e.FromProcess, e.FromEndpoint, e.ToProcess, e.ToEndpoint))
                .ToList();
        }

        internal List<ConnectionInfo> GetConnectionsToProcess(string processName)
        {
            return
                ConnectionTable.GetAllConnectionsToProcess(_connectionTable, processName)
                    .Select(e => new ConnectionInfo(e.FromProcess, e.FromEndpoint, e.ToProcess, e.ToEndpoint))
                    .ToList();
        }

        private CloudTable CreateTableIfNotExists(string tableName, CloudTableClient _tableClient)
        {
            CloudTable table = _tableClient.GetTableReference(tableName);
            try
            {
                table.CreateIfNotExists();
            }
            catch { }

            return table;
        }
    }
}
