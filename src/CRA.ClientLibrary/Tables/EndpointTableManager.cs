using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace CRA.ClientLibrary
{
    /// <summary>
    /// An assignment of one machine to a group
    /// </summary>
    public class EndpointTableManager
    {
        private CloudTable _endpointTable;

        internal EndpointTableManager(string storageConnectionString)
        {
            var _storageAccount = CloudStorageAccount.Parse(storageConnectionString);
            var _tableClient = _storageAccount.CreateCloudTableClient();
            _endpointTable = CreateTableIfNotExists("endpointtableforcra", _tableClient);
        }

        internal void DeleteTable()
        {
            _endpointTable.DeleteIfExists();
        }

        internal bool ExistsEndpoint(string processName, string endPoint)
        {
            TableQuery<EndpointTable> query = new TableQuery<EndpointTable>()
                .Where(TableQuery.CombineFilters(
                            TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, processName),
                            TableOperators.And,
                            TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, endPoint)));
            return _endpointTable.ExecuteQuery(query).Any();
        }

        internal void AddEndpoint(string processName, string endpointName, bool isInput, bool isAsync)
        {
            // Make the connection information stable
            var newRow = new EndpointTable(processName, endpointName, isInput, isAsync);
            TableOperation insertOperation = TableOperation.InsertOrReplace(newRow);
            _endpointTable.Execute(insertOperation);
        }

        internal void DeleteEndpoint(string processName, string endpointName)
        {
            // Make the connection information stable
            var newRow = new DynamicTableEntity(processName, endpointName);
            newRow.ETag = "*";
            TableOperation deleteOperation = TableOperation.Delete(newRow);
            _endpointTable.Execute(deleteOperation);
        }

        internal void RemoveEndpoint(string processName, string endpointName)
        {
            var op = TableOperation.Retrieve<EndpointTable>(processName, endpointName);
            TableResult retrievedResult = _endpointTable.Execute(op);

            // Assign the result to a CustomerEntity.
            var deleteEntity = (EndpointTable)retrievedResult.Result;

            // Create the Delete TableOperation.
            if (deleteEntity != null)
            {
                TableOperation deleteOperation = TableOperation.Delete(deleteEntity);

                // Execute the operation.
                _endpointTable.Execute(deleteOperation);
            }
            else
            {
                Console.WriteLine("Could not retrieve the entity.");
            }
        }

        internal List<string> GetInputEndpoints(string processName)
        {
            TableQuery<EndpointTable> query = new TableQuery<EndpointTable>()
                .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, processName));
            return _endpointTable.ExecuteQuery(query).Where(e => e.IsInput).Select(e => e.EndpointName).ToList();
        }

        internal List<string> GetOutputEndpoints(string processName)
        {
            TableQuery<EndpointTable> query = new TableQuery<EndpointTable>()
                .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, processName));
            return _endpointTable.ExecuteQuery(query).Where(e => !e.IsInput).Select(e => e.EndpointName).ToList();
        }

        private CloudTable CreateTableIfNotExists(string tableName, CloudTableClient _tableClient)
        {
            CloudTable table = _tableClient.GetTableReference(tableName);
            try
            {
                table.CreateIfNotExists();
            }
            catch
            {
            }

            return table;
        }
    }
}
