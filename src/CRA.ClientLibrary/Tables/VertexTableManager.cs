using System;
using System.Linq;
using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using CRA.ClientLibrary.DataProvider;
using System.Threading.Tasks;

namespace CRA.ClientLibrary
{
    /// <summary>
    /// An assignment of one machine to a group
    /// </summary>
    public class VertexTableManager
    {
        private IVertexInfoProvider _vertexInfoProvider;

        internal VertexTableManager(IDataProvider dataProvider)
            => _vertexInfoProvider = dataProvider.GetVertexInfoProvider();

        public IVertexInfoProvider VertexInfoProvider
            => _vertexInfoProvider;

        internal Task DeleteTable()
            => _vertexInfoProvider.DeleteStore();

        internal async Task<bool> ExistsVertex(string vertexName)
            => (await _vertexInfoProvider
                .GetRowsForVertex(vertexName))
                .Any();

        internal async Task<List<int>> ExistsShardedVertex(string vertexName)
            => (await _vertexInfoProvider.GetRowsForShardedVertex(vertexName))
                .Select(vi => int.Parse(vi.VertexName.Split('$')[1]))
                .ToList();

        internal Task RegisterInstance(string instanceName, string address, int port)
            => _vertexInfoProvider.InsertOrReplace(
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
            => _vertexInfoProvider.InsertOrReplace(
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
                .First()
                .Activate();

            await _vertexInfoProvider.InsertOrReplace(newActiveVertex);
        }

        internal async Task DeactivateVertexOnInstance(
            string vertexName,
            string instanceName)
        {
            var newActiveVertex = (await _vertexInfoProvider.GetAll())
                .Where(gn => instanceName == gn.InstanceName
                    && vertexName == gn.VertexName)
                .First()
                .Deactivate();

            await _vertexInfoProvider.InsertOrReplace(newActiveVertex);
        }

        internal void DeleteInstance(string instanceName)
            => _vertexInfoProvider.DeleteVertexInfo(instanceName, string.Empty);

        internal Task DeleteInstanceVertex(string instanceName, string vertexName)
            => _vertexInfoProvider.DeleteVertexInfo(instanceName, vertexName);

        internal async Task DeleteShardedVertex(string vertexName)
        {
            foreach (var row in await _vertexInfoProvider.GetRowsForShardedVertex(vertexName))
            { await _vertexInfoProvider.DeleteVertexInfo(row); }
        }

        internal async Task DeleteShardedVertex(string vertexName, string instanceName)
        {
            foreach (var row in (await _vertexInfoProvider.GetRowsForShardedVertex(vertexName))
                .Where(vi => vi.InstanceName == instanceName))
            { await _vertexInfoProvider.DeleteVertexInfo(row); }
        }

        internal Task<VertexInfo> GetRowForActiveVertex(string vertexName)
            => _vertexInfoProvider.GetRowForVertex(vertexName);

        internal Task<VertexInfo> GetRowForInstance(string instanceName)
            => GetRowForInstanceVertex(instanceName, "");

        internal Task<VertexInfo> GetRowForInstanceVertex(
            string instanceName,
            string vertexName)
            => _vertexInfoProvider.GetRowForInstanceVertex(
                instanceName,
                vertexName);

        internal async Task<VertexInfo> GetRowForDefaultInstance()
            => (await _vertexInfoProvider.GetAll())
                .Where(gn => string.IsNullOrEmpty(gn.VertexName))
                .First();

        internal Task<IEnumerable<string>> GetVertexNames()
            => _vertexInfoProvider.GetVertexNames();

        internal Task<IEnumerable<string>> GetVertexDefinitions()
            => _vertexInfoProvider.GetVertexDefinitions();

        internal Task<IEnumerable<string>> GetInstanceNames()
            => _vertexInfoProvider.GetInstanceNames();
    }
}
