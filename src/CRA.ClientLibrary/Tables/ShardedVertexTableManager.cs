using System;
using System.Linq;
using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System.Linq.Expressions;

namespace CRA.ClientLibrary
{
    /// <summary>
    /// An assignment of one machine to a group
    /// </summary>
    public class ShardedVertexTableManager
    {
        private CloudTable _shardedVertexTable;
        private VertexTableManager _vertexTableManager;

        internal ShardedVertexTableManager(string storageConnectionString)
        {
            var _storageAccount = CloudStorageAccount.Parse(storageConnectionString);
            var _tableClient = _storageAccount.CreateCloudTableClient();
            _shardedVertexTable = CreateTableIfNotExists("crashardedvertextable", _tableClient);
            _vertexTableManager = new VertexTableManager(storageConnectionString);
        }

        internal void DeleteTable()
        {
            _shardedVertexTable.DeleteIfExists();
        }

        internal void RegisterShardedVertex(string vertexName, List<string> allInstances,
            List<int> allShards, List<int> addedShards, List<int> removedShards, Expression<Func<int, int>> shardLocator)
        {
            TableOperation insertOperation = TableOperation.InsertOrReplace(new ShardedVertexTable
                (vertexName, "0", allInstances, allShards, addedShards, removedShards, shardLocator));
            _shardedVertexTable.Execute(insertOperation);
        }

        internal bool ExistsShardedVertex(string vertexName)
        {
            TableQuery<VertexTable> query = new TableQuery<VertexTable>()
                .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, vertexName));
            return _shardedVertexTable.ExecuteQuery(query).Any();
        }

        internal void GetLatestShardedVertex(string vertexName, out List<string> AllInstances, out List<int> AllShards, out List<int> RemovedShards, out List<int> AddedShards)
        {
            var entry = ShardedVertexTable.GetLatestEntryForVertex(_shardedVertexTable, vertexName);
            AllInstances = entry.AllInstances.Split(';').ToList();
            AllShards = entry.AllShards.Split(';').Select(e => Int32.Parse(e)).ToList();
            RemovedShards = entry.RemovedShards.Split(';').Select(e => Int32.Parse(e)).ToList();
            AddedShards = entry.AddedShards.Split(';').Select(e => Int32.Parse(e)).ToList();
        }

        internal ShardingInfo GetLatestShardingInfo(string vertexName)
        {
            ShardingInfo result = new ShardingInfo();

            if (ShardedVertexTable.GetEntriesForVertex(_shardedVertexTable, vertexName).Count() == 0)
            {
                return result;
            }

            var entry = ShardedVertexTable.GetLatestEntryForVertex(_shardedVertexTable, vertexName);
            result.AllShards = entry.AllShards.Split(';').Select(e => Int32.Parse(e)).ToArray();
            result.RemovedShards = new int[0];
            if (entry.RemovedShards != "")
                result.RemovedShards = entry.RemovedShards.Split(';').Select(e => Int32.Parse(e)).ToArray();
            result.AddedShards = new int[0];
            if (entry.AddedShards != "")
                result.AddedShards = entry.AddedShards.Split(';').Select(e => Int32.Parse(e)).ToArray();
            result.ShardLocator = entry.GetShardLocatorExpr();
            return result;
        }

        internal void DeleteShardedVertex(string vertexName)
        {
            _vertexTableManager.DeleteShardedVertex(vertexName);
            foreach (var entry in ShardedVertexTable.GetEntriesForVertex(_shardedVertexTable, vertexName))
            {
                TableOperation deleteOperation = TableOperation.Delete(entry);
                _shardedVertexTable.Execute(deleteOperation);
            }
        }

        private static CloudTable CreateTableIfNotExists(string tableName, CloudTableClient _tableClient)
        {
            CloudTable table = _tableClient.GetTableReference(tableName);
            try
            {
                table.CreateIfNotExists();
            }
            catch (Exception)
            {
            }

            return table;
        }
    }
}
