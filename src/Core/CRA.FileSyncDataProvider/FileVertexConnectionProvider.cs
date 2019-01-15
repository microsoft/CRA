//-----------------------------------------------------------------------
// <copyright file="FileVertexConnectionProvider.cs" company="">
//     Copyright (c) . All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

namespace CRA.FileSyncDataProvider
{
    using CRA.ClientLibrary.DataProvider;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    /// <summary>
    /// Definition for FileVertexConnectionProvider
    /// </summary>
    public class FileVertexConnectionProvider : IVertexConnectionInfoProvider
    {
        private readonly string _fileName;

        public FileVertexConnectionProvider(string fileName)
        { _fileName = fileName; }

        public Task Add(VertexConnectionInfo vertexConnectionInfo)
            => FileUtils.InsertOrUpdate<VertexConnectionInfo>(
                _fileName,
                vertexConnectionInfo,
                MatchVersion,
                UpdateVerion);

        public Task<bool> ContainsRow(VertexConnectionInfo entity)
            => FileUtils.Exists(
                _fileName,
                entity,
                MatchVersion);

        public Task<int> CountAll()
            => FileUtils.CountAll<VertexConnectionInfo>(
                _fileName);

        public Task Delete(VertexConnectionInfo vci)
            => FileUtils.DeleteItem(
                _fileName,
                vci,
                MatchVersion);

        public Task DeleteStore()
        {
            System.IO.File.Delete(_fileName);
            return Task.FromResult(true);
        }

        public Task<VertexConnectionInfo?> Get(string fromVertex, string fromOutput, string toConnection, string toInput)
            => FileUtils.Get<VertexConnectionInfo>(
                _fileName,
                (elem) => elem.FromVertex == fromVertex
                    && elem.FromEndpoint == fromOutput
                    && elem.ToVertex == toConnection
                    && elem.ToEndpoint == toInput);

        public async Task<IEnumerable<VertexConnectionInfo>> GetAll()
            => await FileUtils.GetAll<VertexConnectionInfo>(
                _fileName,
                (e) => true);

        public async Task<IEnumerable<VertexConnectionInfo>> GetAllConnectionsFromVertex(string fromVertex)
            => await FileUtils.GetAll<VertexConnectionInfo>(
                _fileName,
                (e) => e.FromVertex == fromVertex);

        public async Task<IEnumerable<VertexConnectionInfo>> GetAllConnectionsToVertex(string toVertex)
            => await FileUtils.GetAll<VertexConnectionInfo>(
                _fileName,
                (e) => e.ToVertex == toVertex);

        private VertexConnectionInfo UpdateVerion(VertexConnectionInfo vcInfo)
            => new VertexConnectionInfo(
                fromVertex: vcInfo.FromVertex,
                fromEndpoint: vcInfo.FromEndpoint,
                toVertex: vcInfo.ToVertex,
                toEndpoint: vcInfo.ToEndpoint,
                versionId: FileUtils.GetUpdateVersionId(vcInfo.VersionId));

        private (bool matched, bool versionMatched) MatchVersion(VertexConnectionInfo dbItem, VertexConnectionInfo newItem)
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
