//-----------------------------------------------------------------------
// <copyright file="AzureProviderImpl.cs" company="">
//     Copyright (c) . All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

namespace CRA.ClientLibrary.AzureProvider
{
    using CRA.ClientLibrary.DataProvider;
    using Microsoft.WindowsAzure.Storage;
    using Microsoft.WindowsAzure.Storage.Table;
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Definition for AzureProviderImpl
    /// </summary>
    public class AzureProviderImpl : IDataProvider
    {
        private readonly CloudStorageAccount _storageAccount;
        private readonly CloudTableClient _tableClient;
        private readonly string _storageConnectionString;

        public AzureProviderImpl(string storageConnectionString)
        {
            _storageAccount = CloudStorageAccount.Parse(_storageConnectionString);
            _tableClient = _storageAccount.CreateCloudTableClient();
            _storageConnectionString = storageConnectionString;
        }

        public IVertexInfoProvider GetVertexInfoProvider()
            => new AzureVertexInfoProvider(CreateTableIfNotExists("cravertextable"));

        public IEndpointInfoProvider GetEndpointInfoProvider()
            => new AzureEndpointInfoProvider(CreateTableIfNotExists("craendpointtable"));

        public IVertexConnectionInfoProvider GetVertexConnectionInfoProvider()
            => new AzureVertexConnectionInfoProvider(CreateTableIfNotExists("cravertextable"));

        public IShardedVertexInfoProvider GetShardedVertexInfoProvider()
            => new AzureShardedVertexInfoProvider(CreateTableIfNotExists("crashardedvertextable"));

        private CloudTable CreateTableIfNotExists(string tableName)
        {
            CloudTable table = _tableClient.GetTableReference(tableName);
            try
            {
                table.CreateIfNotExistsAsync().Wait();
            }
            catch { }

            return table;
        }

        public IBlobStorageProvider GetBlobStorageProvider()
            => new AzureBlobProvider(_storageAccount.CreateCloudBlobClient(), "cra");

    }
}
