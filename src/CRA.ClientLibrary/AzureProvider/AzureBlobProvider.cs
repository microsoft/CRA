namespace CRA.DataProvider.Azure
{
    using CRA.DataProvider;
    using Microsoft.WindowsAzure.Storage.Blob;
    using System;
    using System.IO;
    using System.Threading.Tasks;

    /// <summary>
    /// Definition for AzureBlobProvider
    /// </summary>
    public class AzureBlobProvider
        : IBlobStorageProvider
    {
        private readonly CloudBlobClient _blobClient;
        private readonly string _parentBlobName;

        public AzureBlobProvider(
            CloudBlobClient blobClient,
            string parentBlobName)
        {
            _blobClient = blobClient;
            _parentBlobName = parentBlobName;
        }

        public async Task Delete(string pathKey)
            => await (await CreateBlockBlobAsync(pathKey)).DeleteIfExistsAsync();

        public async Task<Stream> GetReadStream(string pathKey)
            => await (await CreateBlockBlobAsync(pathKey)).OpenReadAsync();

        public async Task<Stream> GetWriteStream(string pathKey)
            => await (await CreateBlockBlobAsync(pathKey)).OpenWriteAsync();

        private async Task<CloudBlockBlob> CreateBlockBlobAsync(string pathKey)
        {
            CloudBlobContainer container = _blobClient
                .GetContainerReference(_parentBlobName);
            await container.CreateIfNotExistsAsync();
            return container.GetBlockBlobReference(pathKey);
        }
    }
}
