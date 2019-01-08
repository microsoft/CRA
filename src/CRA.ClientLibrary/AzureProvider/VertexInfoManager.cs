//-----------------------------------------------------------------------
// <copyright file="VertexInfoManager.cs" company="">
//     Copyright (c) . All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

namespace CRA.ClientLibrary.AzureProvider
{
    using CRA.ClientLibrary.DataProvider;
    using Microsoft.WindowsAzure.Storage.Table;
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;

    /// <summary>
    /// Definition for VertexInfoManager
    /// </summary>
    public class VertexInfoManager
    {
        private IVertexInfoProvider _vertexInfoProvider;

        internal VertexInfoManager(IDataProvider azureImpl)
        {
            _vertexInfoProvider = azureImpl.GetVertexInfoProvider();
        }

        internal Task DeleteStore()
            => _vertexInfoProvider.Delete();

        internal async Task<bool> ExistsVertex(string vertexName)
        {
            TableQuery<VertexTable> query = new TableQuery<VertexTable>()
                .Where(TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, vertexName));
            return (await _vertexInfoProvider.GetRowsForVertex(vertexName)).Any();
        }

        internal async Task<List<int>> ExistsShardedVertex(string vertexName)
            => (await _vertexInfoProvider.GetRowsForShardedVertex(vertexName))
                .Select(vi => int.Parse(vi.VertexName.Split('$')[1]))
                .ToList();

        internal Task RegisterInstance(string instanceName, string address, int port)
            => _vertexInfoProvider.RegisterVertexInfo(
                new VertexInfo(
                    instanceName: instanceName,
                    address: address,
                    port: port,
                    vertexName: string.Empty,
                    vertexDefinition: string.Empty,
                    vertexCreateAction: string.Empty,
                    vertexParameter: string.Empty,
                    isActive: true,
                    isSharded: false));

        internal void RegisterVertex(string vertexName, string instanceName)
            => _vertexInfoProvider.RegisterVertexInfo(
                new VertexInfo(
                    instanceName: instanceName,
                    address: string.Empty,
                    port: 0,
                    vertexName: vertexName,
                    vertexDefinition: string.Empty,
                    vertexCreateAction: string.Empty,
                    vertexParameter: string.Empty,
                    isActive: false,
                    isSharded: false));

        internal async Task ActivateVertexOnInstance(string vertexName, string instanceName)
        {
            var newActiveVertex = (await _vertexInfoProvider.GetAll())
                .Where(gn => instanceName == gn.InstanceName && vertexName == gn.VertexName)
                .First();

            newActiveVertex = newActiveVertex.Activate();

            await _vertexInfoProvider.UpdateVertex(newActiveVertex);

            var procs = (await _vertexInfoProvider.GetAll())
                .Where(gn => vertexName == gn.VertexName && instanceName != gn.InstanceName);

            foreach (var proc in procs)
            {
                if (proc.IsActive)
                {
                    var updateVertex = proc.Deactivate();
                    await _vertexInfoProvider.UpdateVertex(updateVertex);
                }
            }
        }

        internal async Task DeactivateVertexOnInstance(string vertexName, string instanceName)
        {
            var newActiveVertex = (await _vertexInfoProvider.GetAll())
                .Where(gn => instanceName == gn.InstanceName && vertexName == gn.VertexName)
                .First()
                .Activate();

            await _vertexInfoProvider.UpdateVertex(newActiveVertex);
        }

        internal void DeleteInstance(string instanceName)
            => _vertexInfoProvider.DeleteVertexInfo(instanceName, string.Empty);

        internal Task DeleteInstanceVertex(string instanceName, string vertexName)
            => _vertexInfoProvider.DeleteVertexInfo(instanceName, vertexName);

        internal async Task DeleteShardedVertex(string vertexName)
        {
            foreach (var row in await _vertexInfoProvider.GetRowsForShardedVertex(vertexName))
            {
                await _vertexInfoProvider.DeleteVertexInfo(
                    row.InstanceName,
                    row.VertexName);
            }
        }

        internal Task<VertexInfo> GetRowForActiveVertex(string vertexName)
            => _vertexInfoProvider.GetRowForVertex(vertexName);

        internal Task<VertexInfo> GetRowForInstance(string instanceName)
            => GetRowForInstanceVertex(instanceName, "");

        internal Task<VertexInfo> GetRowForInstanceVertex(string instanceName, string vertexName)
            => _vertexInfoProvider.GetRowForInstanceVertex(instanceName, vertexName);

        internal async Task<VertexTable> GetRowForDefaultInstance()
            => (await _vertexInfoProvider.GetAll())
                .Where(gn => string.IsNullOrEmpty(gn.VertexName))
                .First();

        private static CloudTable CreateTableIfNotExists(string tableName, CloudTableClient _tableClient)
        {
            CloudTable table = _tableClient.GetTableReference(tableName);
            try
            {
                table.CreateIfNotExistsAsync().Wait();
            }
            catch (Exception)
            {
            }

            return table;
        }

        internal Task<List<string>> GetVertexNames()
            => _vertexInfoProvider.GetVertexNames();

        internal Task<List<string>> GetVertexDefinitions()
            => _vertexInfoProvider.GetVertexDefinitions();

        internal Task<List<string>> GetInstanceNames()
            => _vertexInfoProvider.GetInstanceNames();
    }
}
