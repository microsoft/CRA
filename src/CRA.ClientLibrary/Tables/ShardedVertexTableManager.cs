using System;
using System.Linq;
using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System.Linq.Expressions;
using CRA.ClientLibrary.DataProvider;
using System.Threading.Tasks;

namespace CRA.ClientLibrary
{
    /// <summary>
    /// An assignment of one machine to a group
    /// </summary>
    public class ShardedVertexTableManager
    {
        IShardedVertexInfoProvider _shardedVertexInfoProvider;
        private VertexTableManager _vertexTableManager;

        public ShardedVertexTableManager(IDataProvider azureImpl)
        {
            _shardedVertexInfoProvider = azureImpl.GetShardedVertexInfoProvider();
            _vertexTableManager = new VertexTableManager(azureImpl);
        }

        public async Task DeleteTableAsync()
            => await _shardedVertexInfoProvider.Delete();

        public async Task RegisterShardedVertexAsync(
            string vertexName,
            List<string> allInstances,
            List<int> allShards,
            List<int> addedShards,
            List<int> removedShards,
            Expression<Func<int, int>> shardLocator)
        {
            await _shardedVertexInfoProvider.Insert(
                ShardedVertexInfo.Create(
                    vertexName,
                    "0",
                    allInstances,
                    allShards,
                    addedShards,
                    removedShards,
                    shardLocator));
        }

        public async Task<bool> ExistsShardedVertex(string vertexName)
            => (await _shardedVertexInfoProvider.GetEntriesForVertex(vertexName)).Any();

        public async Task<(List<string> allInstances, List<int> allShards, List<int> removeShards, List<int> addesShards)>
            GetLatestShardedVertex(string vertexName)
        {
            var entry = await _shardedVertexInfoProvider.GetLatestEntryForVertex(vertexName);
            var allInstances = entry.AllInstances.Split(';').ToList();
            var allShards = entry.AllShards.Split(';').Select(e => Int32.Parse(e)).ToList();
            var removedShards = entry.RemovedShards.Split(';').Select(e => Int32.Parse(e)).ToList();
            var addedShards = entry.AddedShards.Split(';').Select(e => Int32.Parse(e)).ToList();

            return (allInstances, allShards, removedShards, addedShards);
        }

        public async Task<ShardingInfo> GetLatestShardingInfo(string vertexName)
        {
            ShardingInfo result = new ShardingInfo();

            if (await this.ExistsShardedVertex(vertexName))
            { return result; }

            var entry = await _shardedVertexInfoProvider.GetLatestEntryForVertex(vertexName);
            result.AllShards = entry.AllShards.Split(';').Select(e => Int32.Parse(e)).ToArray();

            result.RemovedShards = new int[0];
            if (entry.RemovedShards != "")
            { result.RemovedShards = entry.RemovedShards.Split(';').Select(e => Int32.Parse(e)).ToArray(); }

            result.AddedShards = new int[0];

            if (entry.AddedShards != "")
            { result.AddedShards = entry.AddedShards.Split(';').Select(e => Int32.Parse(e)).ToArray(); }

            result.ShardLocator = entry.GetShardLocatorExpr();
            return result;
        }

        public async Task DeleteShardedVertex(string vertexName)
        {
            await _vertexTableManager.DeleteShardedVertex(vertexName, "");

            foreach (var entry in await _shardedVertexInfoProvider.GetEntriesForVertex(vertexName))
            { await _shardedVertexInfoProvider.Delete(entry); }
        }
    }
}
