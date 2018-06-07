using System;
using System.Linq;
using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace CRA.ClientLibrary
{
    /// <summary>
    /// An assignment of one machine to a group
    /// </summary>
    public class VertexTableManager
    {
        private CloudTable _vertexTable;

        internal VertexTableManager(string storageConnectionString)
        {
            var _storageAccount = CloudStorageAccount.Parse(storageConnectionString);
            var _tableClient = _storageAccount.CreateCloudTableClient();
            _vertexTable = CreateTableIfNotExists("cravertextable", _tableClient);
        }

        internal void DeleteTable()
        {
            _vertexTable.DeleteIfExistsAsync().Wait();
        }

        internal bool ExistsVertex(string vertexName)
        {
            TableQuery<VertexTable> query = new TableQuery<VertexTable>()
                .Where(TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, vertexName));
            return _vertexTable.ExecuteQuery(query).Any();
        }

        internal bool ExistsShardedVertex(string vertexName, out List<int> shards)
        {
            TableQuery<VertexTable> query = new TableQuery<VertexTable>()
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
            shards = _vertexTable.ExecuteQuery(query)
                .Select(e => int.Parse(e.VertexName.Split('$')[1])).ToList();

            return shards.Count > 0;
        }

        internal void RegisterInstance(string instanceName, string address, int port)
        {
            TableOperation insertOperation = TableOperation.InsertOrReplace(new VertexTable
                (instanceName, "", "", address, port, "", "", true, false));
            _vertexTable.ExecuteAsync(insertOperation).Wait();
        }

        internal void RegisterVertex(string vertexName, string instanceName)
        {
            TableOperation insertOperation = TableOperation.InsertOrReplace(new VertexTable
                (instanceName, vertexName, "", "", 0, "", "", false, false));
            _vertexTable.ExecuteAsync(insertOperation).Wait();
        }

        internal void ActivateVertexOnInstance(string vertexName, string instanceName)
        {
            var newActiveVertex = VertexTable.GetAll(_vertexTable)
                .Where(gn => instanceName == gn.InstanceName && vertexName == gn.VertexName)
                .First();

            newActiveVertex.IsActive = true;
            TableOperation insertOperation = TableOperation.InsertOrReplace(newActiveVertex);
            _vertexTable.ExecuteAsync(insertOperation).Wait();

            var procs = VertexTable.GetAll(_vertexTable)
                .Where(gn => vertexName == gn.VertexName && instanceName != gn.InstanceName);
            foreach (var proc in procs)
            {
                if (proc.IsActive)
                {
                    proc.IsActive = false;
                    TableOperation _insertOperation = TableOperation.InsertOrReplace(proc);
                    _vertexTable.ExecuteAsync(_insertOperation).Wait();
                }
            }
        }

        internal void DeactivateVertexOnInstance(string vertexName, string instanceName)
        {
            var newActiveVertex = VertexTable.GetAll(_vertexTable)
                .Where(gn => instanceName == gn.InstanceName && vertexName == gn.VertexName)
                .First();

            newActiveVertex.IsActive = false;
            TableOperation insertOperation = TableOperation.InsertOrReplace(newActiveVertex);
            _vertexTable.ExecuteAsync(insertOperation).Wait();
        }

        internal void DeleteInstance(string instanceName)
        {
            var newRow = new DynamicTableEntity(instanceName, "");
            newRow.ETag = "*";
            TableOperation deleteOperation = TableOperation.Delete(newRow);
            _vertexTable.ExecuteAsync(deleteOperation).Wait();
        }

        internal void DeleteInstanceVertex(string instanceName, string vertexName)
        {
            var newRow = new DynamicTableEntity(instanceName, vertexName);
            newRow.ETag = "*";
            TableOperation deleteOperation = TableOperation.Delete(newRow);
            _vertexTable.ExecuteAsync(deleteOperation).Wait();
        }

        internal void DeleteShardedVertex(string vertexName)
        {
            foreach (var row in VertexTable.GetRowsForShardedVertex(_vertexTable, vertexName))
            {
                TableOperation deleteOperation = TableOperation.Delete(row);
                _vertexTable.ExecuteAsync(deleteOperation).Wait();
            }
        }

        internal VertexTable GetRowForActiveVertex(string vertexName)
        {
            return VertexTable.GetAll(_vertexTable)
                .Where(gn => vertexName == gn.VertexName && !string.IsNullOrEmpty(gn.InstanceName))
                .Where(gn => gn.IsActive)
                .First();
        }

        internal VertexTable GetRowForInstance(string instanceName)
        {
            return GetRowForInstanceVertex(instanceName, "");
        }

        internal VertexTable GetRowForInstanceVertex(string instanceName, string vertexName)
        {
            return VertexTable.GetAll(_vertexTable).Where(gn => instanceName == gn.InstanceName && vertexName == gn.VertexName).First();
        }

        internal VertexTable GetRowForDefaultInstance()
        {
            return VertexTable.GetAll(_vertexTable).Where(gn => string.IsNullOrEmpty(gn.VertexName)).First();
        }

        private static CloudTable CreateTableIfNotExists(string tableName, CloudTableClient _tableClient)
        {
            CloudTable table = _tableClient.GetTableReference(tableName);
            try
            {
                table.CreateIfNotExistsAsync().Wait();
            }
            catch (Exception)
            {
            }

            return table;
        }

        internal List<string> GetVertexNames()
        {
            TableQuery<VertexTable> query = new TableQuery<VertexTable>()
                .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.NotEqual, ""));
            return _vertexTable.ExecuteQuery(query).Select(e => e.VertexName).ToList();
        }

        internal List<string> GetVertexDefinitions()
        {
            TableQuery<VertexTable> query = new TableQuery<VertexTable>()
                .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, ""));
            return _vertexTable.ExecuteQuery(query).Select(e => e.VertexName).ToList();
        }

        internal List<string> GetInstanceNames()
        {
            TableQuery<VertexTable> query = new TableQuery<VertexTable>()
                .Where(TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, ""));
            return _vertexTable.ExecuteQuery(query).Select(e => e.InstanceName).ToList();
        }
    }
}
