namespace CRA.FileSyncDataProvider
{
    using CRA.ClientLibrary.DataProvider;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;

    /// <summary>
    /// Definition for FileEndpointProvider
    /// </summary>
    public class FileEndpointProvider : IEndpointInfoProvider
    {
        private readonly string _fileName;

        public FileEndpointProvider(string fileName)
        { _fileName = fileName; }

        public Task AddEndpoint(EndpointInfo endpointInfo)
            => FileUtils.InsertOrUpdate(
                _fileName,
                endpointInfo,
                MatchVersion,
                UpdateVerion);

        public Task DeleteEndpoint(string vertexName, string endpointName, string versionId = "*")
            => DeleteEndpoint(
                new EndpointInfo(
                    vertexName,
                    endpointName,
                    false,
                    false,
                    versionId));

        public Task DeleteEndpoint(EndpointInfo endpointInfo)
            => FileUtils.DeleteItem(
                _fileName,
                endpointInfo,
                MatchVersion);

        public Task DeleteStore()
        {
            System.IO.File.Delete(_fileName);
            return Task.FromResult(true);
        }

        public Task<bool> ExistsEndpoint(string vertexName, string endPoint)
            => FileUtils.Exists(
                _fileName,
                new EndpointInfo(
                    vertexName,
                    endPoint,
                    false,
                    false,
                    null),
                MatchVersion);

        public async Task<IEnumerable<EndpointInfo>> GetAll()
            => await FileUtils.GetAll<EndpointInfo>(
                _fileName,
                (e) => true);

        public Task<EndpointInfo?> GetEndpoint(string vertexName, string endpointName)
            => FileUtils.Get<EndpointInfo>(
                _fileName,
                (e) => e.VersionId == vertexName && e.EndpointName == endpointName);

        public Task<List<EndpointInfo>> GetEndpoints(string vertexName)
            => FileUtils.GetAll<EndpointInfo>(
                _fileName,
                (e) => e.VertexName == vertexName);

        public Task<List<EndpointInfo>> GetShardedEndpoints(string vertexName, string endpointName)
            => FileUtils.GetAll<EndpointInfo>(
                _fileName,
                (e) => e.VertexName.StartsWith(vertexName + "#")
                    && e.EndpointName == endpointName);

        private EndpointInfo UpdateVerion(EndpointInfo endpointInfo)
            => new EndpointInfo(
                vertexName: endpointInfo.VertexName,
                endpointName: endpointInfo.EndpointName,
                isInput: endpointInfo.IsInput,
                isAsync: endpointInfo.IsAsync,
                versionId: FileUtils.GetUpdateVersionId(endpointInfo.VersionId));

        private (bool matched, bool versionMatched) MatchVersion(EndpointInfo dbItem, EndpointInfo newItem)
        {
            if (dbItem.VertexName == newItem.VertexName
                && dbItem.EndpointName == newItem.EndpointName)
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
