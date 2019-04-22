namespace CRA.ClientLibrary.AzureProvider
{
    using CRA.ClientLibrary.DataProvider;
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
            => await (await this.CreateBlockBlob(pathKey))
            .DeleteIfExistsAsync();

        public async Task<Stream> GetReadStream(string pathKey)
            => await (await this.CreateBlockBlob(pathKey))
            .OpenReadAsync();

        public async Task<Stream> GetWriteStream(string pathKey)
            => await (await this.CreateBlockBlob(pathKey))
            .OpenWriteAsync();

        private async Task<CloudBlockBlob> CreateBlockBlob(string pathKey)
        {
            CloudBlobContainer container = _blobClient
                .GetContainerReference(_parentBlobName);
            //await container.CreateIfNotExistsAsync();
            container.CreateIfNotExistsAsync().Wait();

            return container.GetBlockBlobReference(pathKey);
        }
    }
}
