namespace CRA.DataProvider.File
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using CRA.DataProvider;

    /// <summary>
    /// Definition for FileVertexProvider
    /// </summary>
    public class FileVertexProvider
        : IVertexInfoProvider
    {
        private readonly string _fileName;

        public FileVertexProvider(string fileName)
        { _fileName = fileName; }

        public async Task<bool> ContainsInstance(string instanceName)
            => (await FileUtils.GetAll<VertexInfo>(
                _fileName,
                (vi) => vi.InstanceName == instanceName)).Count == 0;

        public Task<bool> ContainsRow(VertexInfo entity)
            => FileUtils.Exists(
                _fileName,
                entity,
                MatchVersion);

        public Task<int> CountAll()
            => FileUtils.CountAll<VertexInfo>(_fileName);

        public Task DeleteStore()
        {
            System.IO.File.Delete(_fileName);
            return Task.FromResult(true);
        }

        public async Task DeleteVertexInfo(string instanceName, string vertexName)
        {
            var item = await FileUtils.Get<VertexInfo>(
                _fileName,
                (vi) => vi.InstanceName == instanceName && vi.VertexName == vertexName);

            if (item.HasValue)
            {
                await FileUtils.DeleteItem(
                    _fileName,
                    item.Value,
                    MatchVersion);
            }
        }

        public Task DeleteVertexInfo(VertexInfo vertexInfo)
            => FileUtils.DeleteItem(
                    _fileName,
                    vertexInfo,
                    MatchVersion);

        public async Task<IEnumerable<VertexInfo>> GetAll()
            => await FileUtils.GetAll<VertexInfo>(
                _fileName,
                (vi) => true);

        public async Task<IEnumerable<VertexInfo>> GetAllRowsForInstance(string instanceName)
            => (await FileUtils.GetAll<VertexInfo>(
                _fileName,
                (vi) => vi.InstanceName == instanceName));

        public async Task<VertexInfo?> GetInstanceFromAddress(string address, int port)
            => (await FileUtils.Get<VertexInfo>(
                _fileName,
                (vi) => vi.Address == address && vi.Port == port));

        public async Task<IEnumerable<string>> GetInstanceNames()
            => (await this.GetAll())
            .Where(_ => _.VertexName == "")
            .Select(_ => _.InstanceName);

        public async Task<VertexInfo?> GetRowForInstance(string instanceName)
            => (await FileUtils.Get<VertexInfo>(
                _fileName,
                (vi) => vi.InstanceName == instanceName && string.IsNullOrEmpty(vi.VertexName)));

        public async Task<VertexInfo?> GetRowForInstanceVertex(string instanceName, string vertexName)
            => (await FileUtils.Get<VertexInfo>(
                _fileName,
                (vi) => vi.InstanceName == instanceName
                    && (vi.VertexName == vertexName
                        || (string.IsNullOrEmpty(vertexName) && string.IsNullOrEmpty(vi.VertexName)))));

        public async Task<VertexInfo?> GetRowForActiveVertex(string vertexName)
            => (await FileUtils.Get<VertexInfo>(
                _fileName,
                (vi) => vi.VertexName == vertexName && vi.IsActive));

        public async Task<VertexInfo?> GetRowForVertexDefinition(string vertexDefinition)
            => (await FileUtils.Get<VertexInfo>(
                _fileName,
                (vi) => vi.VertexDefinition == vertexDefinition));

        public async Task<IEnumerable<VertexInfo>> GetRowsForShardedInstanceVertex(string instanceName, string vertexName)
            => (await this.GetAll())
                .Where(_ =>
                    _.InstanceName == instanceName
                    && _.VertexName.StartsWith(vertexName + "$"));

        public async Task<IEnumerable<VertexInfo>> GetRowsForShardedVertex(string vertexName)
            => (await this.GetAll())
                .Where(_ => _.VertexName.StartsWith(vertexName + "$"));

        public async Task<IEnumerable<VertexInfo>> GetRowsForVertex(string vertexName)
            => (await this.GetAll())
                .Where(_ => _.VertexName.StartsWith(vertexName));

        public async Task<IEnumerable<string>> GetVertexDefinitions()
            => (await this.GetAll())
                .Where(_ => !string.IsNullOrEmpty(_.VertexDefinition)
                    && string.IsNullOrEmpty(_.InstanceName))
                .Select(_ => _.VertexDefinition);

        public async Task<IEnumerable<string>> GetVertexNames()
            => (await this.GetAll())
                .Where(_ => !string.IsNullOrEmpty(_.InstanceName))
                .Select(_ => _.VertexName);

        public async Task<IEnumerable<VertexInfo>> GetVertices(string instanceName)
            => (await this.GetAll())
                .Where(_ => _.InstanceName == instanceName);

        public Task InsertOrReplace(VertexInfo newInfo)
            => FileUtils.InsertOrUpdate(
                _fileName,
                newInfo,
                MatchVersion,
                UpdateVerion);

        private VertexInfo UpdateVerion(VertexInfo vcInfo)
            => new VertexInfo(
                instanceName: vcInfo.InstanceName,
                address: vcInfo.Address,
                port: vcInfo.Port,
                vertexName: vcInfo.VertexName,
                vertexDefinition: vcInfo.VertexDefinition,
                vertexCreateAction: vcInfo.VertexCreateAction,
                vertexParameter: vcInfo.VertexParameter,
                isActive: vcInfo.IsActive,
                isSharded: vcInfo.IsActive,
                versionId: FileUtils.GetUpdateVersionId(vcInfo.VersionId));

        private (bool matched, bool versionMatched) MatchVersion(VertexInfo dbItem, VertexInfo newItem)
        {
            if (dbItem == newItem)
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
