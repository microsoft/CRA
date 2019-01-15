//-----------------------------------------------------------------------
// <copyright file="FileVertexProvider.cs" company="">
//     Copyright (c) . All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

namespace CRA.FileSyncDataProvider
{
    using CRA.ClientLibrary.DataProvider;
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    /// <summary>
    /// Definition for FileVertexProvider
    /// </summary>
    public class FileVertexProvider
        : IVertexInfoProvider
    {
        private readonly string _fileName;

        public FileVertexProvider(string fileName)
        {
            _fileName = fileName;
        }
        public Task<bool> ContainsInstance(string instanceName) => throw new NotImplementedException();
        public Task<bool> ContainsRow(VertexInfo entity) => throw new NotImplementedException();
        public Task<int> CountAll() => throw new NotImplementedException();
        public Task DeleteStore() => throw new NotImplementedException();
        public Task DeleteVertexInfo(string instanceName, string vertexName) => throw new NotImplementedException();
        public Task DeleteVertexInfo(VertexInfo vertexInfo) => throw new NotImplementedException();
        public Task<IEnumerable<VertexInfo>> GetAll() => throw new NotImplementedException();
        public Task<IEnumerable<VertexInfo>> GetAllRowsForInstance(string instanceName) => throw new NotImplementedException();
        public Task<VertexInfo> GetInstanceFromAddress(string address, int port) => throw new NotImplementedException();
        public Task<IEnumerable<string>> GetInstanceNames() => throw new NotImplementedException();
        public Task<VertexInfo> GetRowForInstance(string instanceName) => throw new NotImplementedException();
        public Task<VertexInfo> GetRowForInstanceVertex(string instanceName, string vertexName) => throw new NotImplementedException();
        public Task<VertexInfo> GetRowForVertex(string vertexName) => throw new NotImplementedException();
        public Task<VertexInfo> GetRowForVertexDefinition(string vertexDefinition) => throw new NotImplementedException();
        public Task<IEnumerable<VertexInfo>> GetRowsForShardedInstanceVertex(string instanceName, string vertexName) => throw new NotImplementedException();
        public Task<IEnumerable<VertexInfo>> GetRowsForShardedVertex(string vertexName) => throw new NotImplementedException();
        public Task<IEnumerable<VertexInfo>> GetRowsForVertex(string vertexName) => throw new NotImplementedException();
        public Task<IEnumerable<string>> GetVertexDefinitions() => throw new NotImplementedException();
        public Task<IEnumerable<string>> GetVertexNames() => throw new NotImplementedException();
        public Task<IEnumerable<VertexInfo>> GetVertices(string instanceName) => throw new NotImplementedException();
        public Task InsertOrReplace(VertexInfo newInfo) => throw new NotImplementedException();
    }
}
