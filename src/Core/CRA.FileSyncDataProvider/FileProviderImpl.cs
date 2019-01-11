//-----------------------------------------------------------------------
// <copyright file="FileProviderImpl.cs" company="">
//     Copyright (c) . All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

namespace CRA.FileSyncDataProvider
{
    using CRA.ClientLibrary.DataProvider;
    using System;
    using System.Collections.Generic;
    using System.IO;

    /// <summary>
    /// Definition for FileProviderImpl
    /// </summary>
    public class FileProviderImpl : IDataProvider
    {
        private readonly string _directoryPath;

        public FileProviderImpl(string directoryPath)
        { _directoryPath = directoryPath; }

        public IBlobStorageProvider GetBlobStorageProvider()
        {
            return new FileBlobProvider(
                GetDirectory("Blobs"));
        }

        public IEndpointInfoProvider GetEndpointInfoProvider()
            => new FileEndpointProvider(
                Path.Combine(GetDirectory("Data"), "endpoints.json"));

        public IShardedVertexInfoProvider GetShardedVertexInfoProvider()
            => new FileShardedVertexProvider(
                Path.Combine(GetDirectory("Data"), "sharded_vertexes.json"));

        public IVertexConnectionInfoProvider GetVertexConnectionInfoProvider()
            => throw new NotImplementedException();

        public IVertexInfoProvider GetVertexInfoProvider()
            => throw new NotImplementedException();

        private string GetDirectory(string subPath)
        {
            string subDirectoryPath = Path.Combine(_directoryPath, subPath);
            if (Directory.Exists(subDirectoryPath))
            { Directory.CreateDirectory(subDirectoryPath); }

            return subDirectoryPath;
        }
    }
}
