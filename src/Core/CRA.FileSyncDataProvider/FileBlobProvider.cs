//-----------------------------------------------------------------------
// <copyright file="FileBlobProvider.cs" company="">
//     Copyright (c) . All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

namespace CRA.FileSyncDataProvider
{
    using CRA.ClientLibrary.DataProvider;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Threading.Tasks;

    /// <summary>
    /// Definition for FileBlobProvider
    /// </summary>
    public class FileBlobProvider
        : IBlobStorageProvider
    {
        private readonly string _blobDirectory;

        public FileBlobProvider(string blobDirectory)
        { _blobDirectory = FileUtils.GetDirectory(blobDirectory); }

        public Task Delete(string pathKey)
        {
            File.Delete(Path.Combine(_blobDirectory, pathKey));
            return Task.FromResult(true);
        }

        public Task<Stream> GetReadStream(string pathKey)
            => Task.FromResult<Stream>(
                FileUtils.GetReadStream(
                    Path.Combine(
                        _blobDirectory, pathKey)));

        public Task<Stream> GetWriteStream(string pathKey)
            => Task.FromResult<Stream>(
                FileUtils.GetReadWriteStream(
                    Path.Combine(_blobDirectory, pathKey)));
    }
}
