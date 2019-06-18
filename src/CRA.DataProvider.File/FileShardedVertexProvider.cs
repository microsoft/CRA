namespace CRA.DataProvider.File
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using CRA.DataProvider;

    /// <summary>
    /// Definition for FileShardedVertexProvider
    /// </summary>
    public class FileShardedVertexProvider
        : IShardedVertexInfoProvider
    {
        private readonly string _fileName;

        public FileShardedVertexProvider(string fileName)
        { _fileName = fileName; }

        public Task<int> CountAll()
            => FileUtils.CountAll<ShardedVertexInfo>(_fileName);

        public Task Delete()
        {
            System.IO.File.Delete(_fileName);
            return Task.FromResult(true);
        }

        public Task Delete(ShardedVertexInfo entry)
            => FileUtils.DeleteItem(
                _fileName,
                entry,
                MatchVersion);

        public async Task<IEnumerable<ShardedVertexInfo>> GetAll()
            => await FileUtils.GetAll<ShardedVertexInfo>(
                _fileName,
                (e) => true);

        public async Task<IEnumerable<ShardedVertexInfo>> GetEntriesForVertex(string vertexName)
            => await FileUtils.GetAll<ShardedVertexInfo>(
                _fileName,
                (e) => e.VertexName == vertexName);

        public async Task<ShardedVertexInfo> GetEntryForVertex(string vertexName, string epochId)
            => (await FileUtils.Get<ShardedVertexInfo>(
                _fileName,
                (e) => e.VertexName == vertexName && e.EpochId == epochId)).Value;

        public async Task<ShardedVertexInfo> GetLatestEntryForVertex(string vertexName)
            => (await FileUtils.GetAll<ShardedVertexInfo>(
                    _fileName,
                    (e) => e.VertexName == vertexName))
                .OrderByDescending(e => e.EpochId)
                .First();

        public Task Insert(ShardedVertexInfo shardedVertexInfo)
            => FileUtils.InsertOrUpdate(
                _fileName,
                shardedVertexInfo,
                MatchVersion,
                UpdateVerion);

        private ShardedVertexInfo UpdateVerion(ShardedVertexInfo svInfo)
            => new ShardedVertexInfo(
                vertexName: svInfo.VertexName,
                epochId: svInfo.EpochId,
                addedShards: svInfo.AddedShards,
                allShards: svInfo.AllShards,
                allInstances: svInfo.AllInstances,
                removedShards: svInfo.RemovedShards,
                shardLocator: svInfo.ShardLocator,
                versionId: FileUtils.GetUpdateVersionId(svInfo.VersionId));

        private (bool matched, bool versionMatched) MatchVersion(ShardedVertexInfo dbItem, ShardedVertexInfo newItem)
        {
            if (dbItem.VertexName == newItem.VertexName
                && dbItem.EpochId == newItem.EpochId)
            {
                if (newItem.VersionId == null
                    || newItem.VersionId == "*"
                    || newItem.VersionId == dbItem.VersionId)
                {
                    return (true, true);
                }

                return (true, false);
            }

            return (false, false);
        }
    }
}
