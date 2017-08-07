using System;
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
    public class EndpointTableManager
    {
        private CloudTable _endpointTable;

        internal EndpointTableManager(string storageConnectionString)
        {
            var _storageAccount = CloudStorageAccount.Parse(storageConnectionString);
            var _tableClient = _storageAccount.CreateCloudTableClient();
            _endpointTable = CreateTableIfNotExists("endpointtableforcra", _tableClient);
        }

        public CloudTable EndpointTable
        {
            get
            {
                return _endpointTable;
            }
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
                Debug.WriteLine("Creating table " + tableName);
                table.CreateIfNotExists();
            }
            catch { }

            return table;
        }

        /// <summary>
        /// Delete contents of the table.
        /// </summary>
        internal void DeleteContents()
        {
            DeleteContents(_endpointTable);
        }

        /// <summary>
        /// Delete contents of a cloud table
        /// </summary>
        /// <param name="_table"></param>
        private static void DeleteContents(CloudTable table)
        {
            Action<IEnumerable<DynamicTableEntity>> processor = entities =>
            {
                var batches = new Dictionary<string, TableBatchOperation>();

                foreach (var entity in entities)
                {
                    TableBatchOperation batch = null;

                    if (batches.TryGetValue(entity.PartitionKey, out batch) == false)
                    {
                        batches[entity.PartitionKey] = batch = new TableBatchOperation();
                    }

                    batch.Add(TableOperation.Delete(entity));

                    if (batch.Count == 100)
                    {
                        table.ExecuteBatch(batch);
                        batches[entity.PartitionKey] = new TableBatchOperation();
                    }
                }

                foreach (var batch in batches.Values)
                {
                    if (batch.Count > 0)
                    {
                        table.ExecuteBatch(batch);
                    }
                }
            };

            ProcessEntities(table, processor);
        }

        /// <summary>
        /// Process all entities in a cloud table using the given processor lambda.
        /// </summary>
        /// <param name="table"></param>
        /// <param name="processor"></param>
        private static void ProcessEntities(CloudTable table, Action<IEnumerable<DynamicTableEntity>> processor)
        {
            TableQuerySegment<DynamicTableEntity> segment = null;

            while (segment == null || segment.ContinuationToken != null)
            {
                segment = table.ExecuteQuerySegmented(new TableQuery().Take(100), segment == null ? null : segment.ContinuationToken);
                processor(segment.Results);
            }
        }
    }
}
