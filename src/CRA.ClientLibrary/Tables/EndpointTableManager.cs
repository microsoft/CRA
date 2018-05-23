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
            _endpointTable = CreateTableIfNotExists("craendpointtable", _tableClient);
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

        internal bool ExistsEndpoint(string vertexName, string endPoint)
        {
            TableQuery<EndpointTable> query = new TableQuery<EndpointTable>()
                .Where(TableQuery.CombineFilters(
                            TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, vertexName),
                            TableOperators.And,
                            TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, endPoint)));
            return _endpointTable.ExecuteQuery(query).Any();
        }

        internal void AddEndpoint(string vertexName, string endpointName, bool isInput, bool isAsync)
        {
            // Make the connection information stable
            var newRow = new EndpointTable(vertexName, endpointName, isInput, isAsync);
            TableOperation insertOperation = TableOperation.InsertOrReplace(newRow);
            _endpointTable.Execute(insertOperation);
        }

        internal void DeleteEndpoint(string vertexName, string endpointName)
        {
            // Make the connection information stable
            var newRow = new DynamicTableEntity(vertexName, endpointName);
            newRow.ETag = "*";
            TableOperation deleteOperation = TableOperation.Delete(newRow);
            _endpointTable.Execute(deleteOperation);
        }

        internal void RemoveEndpoint(string vertexName, string endpointName)
        {
            var op = TableOperation.Retrieve<EndpointTable>(vertexName, endpointName);
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

        internal List<string> GetInputEndpoints(string vertexName)
        {
            TableQuery<EndpointTable> query = new TableQuery<EndpointTable>()
                .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, vertexName));
            return _endpointTable.ExecuteQuery(query).Where(e => e.IsInput).Select(e => e.EndpointName).ToList();
        }

        internal List<string> GetOutputEndpoints(string vertexName)
        {
            TableQuery<EndpointTable> query = new TableQuery<EndpointTable>()
                .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, vertexName));
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
            Action<IEnumerable<DynamicTableEntity>> vertexor = entities =>
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

            VertexEntities(table, vertexor);
        }

        /// <summary>
        /// Vertex all entities in a cloud table using the given vertexor lambda.
        /// </summary>
        /// <param name="table"></param>
        /// <param name="vertexor"></param>
        private static void VertexEntities(CloudTable table, Action<IEnumerable<DynamicTableEntity>> vertexor)
        {
            TableQuerySegment<DynamicTableEntity> segment = null;

            while (segment == null || segment.ContinuationToken != null)
            {
                segment = table.ExecuteQuerySegmented(new TableQuery().Take(100), segment == null ? null : segment.ContinuationToken);
                vertexor(segment.Results);
            }
        }
    }
}
