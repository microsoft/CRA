//-----------------------------------------------------------------------
// <copyright file="FileProviderImpl.cs" company="">
//     Copyright (c) . All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

namespace CRA.FileSyncDataProvider
{
    using CRA.ClientLibrary.DataProvider;
    using System;
    using System.IO;

    /// <summary>
    /// Definition for FileProviderImpl
    /// </summary>
    public class FileProviderImpl : IDataProvider
    {
        private readonly string _directoryPath;

        public FileProviderImpl()
        { _directoryPath = FileUtils.GetDirectory(GetDefaultDirectory()); }

        public FileProviderImpl(string directoryPath)
        { _directoryPath = FileUtils.GetDirectory(directoryPath); }

        public IBlobStorageProvider GetBlobStorageProvider()
            => new FileBlobProvider(
                GetDirectory("Blobs"));

        public IEndpointInfoProvider GetEndpointInfoProvider()
            => new FileEndpointProvider(
                Path.Combine(GetDirectory("Data"), "endpoints.json"));

        public IShardedVertexInfoProvider GetShardedVertexInfoProvider()
            => new FileShardedVertexProvider(
                Path.Combine(GetDirectory("Data"), "sharded_vertexes.json"));

        public IVertexConnectionInfoProvider GetVertexConnectionInfoProvider()
            => new FileVertexConnectionProvider(
                Path.Combine(GetDirectory("Data"), "vertex_connections.json"));

        public IVertexInfoProvider GetVertexInfoProvider()
            => new FileVertexProvider(
                Path.Combine(GetDirectory("Data"), "vertex.json"));

        public static string GetDefaultDirectory()
            => Path.Combine(
                Environment.GetEnvironmentVariable("TEMP"),
                "CRA");

        private string GetDirectory(string subPath)
        {
            string subDirectoryPath = Path.Combine(_directoryPath, subPath);
            return FileUtils.GetDirectory(subPath);
        }
    }
}
