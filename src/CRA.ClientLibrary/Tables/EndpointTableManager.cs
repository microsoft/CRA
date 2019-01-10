using System;
using System.Collections.Generic;
using System.Linq;
using CRA.ClientLibrary.DataProvider;
using System.Threading.Tasks;

namespace CRA.ClientLibrary
{
    /// <summary>
    /// An assignment of one machine to a group
    /// </summary>
    public class EndpointTableManager
    {
        private IEndpointInfoProvider _endpointDataProvider;

        internal EndpointTableManager(IDataProvider dataProvider)
        {
            _endpointDataProvider = dataProvider.GetEndpointInfoProvider();
        }

        internal Task DeleteTable()
            => _endpointDataProvider.DeleteStore();

        internal Task<bool> ExistsEndpoint(string vertexName, string endPoint)
            => _endpointDataProvider.ExistsEndpoint(vertexName, endPoint);

        internal Task AddEndpoint(string vertexName, string endpointName, bool isInput, bool isAsync)
            => _endpointDataProvider.AddEndpoint(
                new EndpointInfo(
                    vertexName: vertexName,
                    endpointName: endpointName,
                    isInput: isInput,
                    isAsync: isAsync));

        internal Task DeleteEndpoint(string vertexName, string endpointName)
            => _endpointDataProvider.DeleteEndpoint(vertexName, endpointName);

        internal async Task RemoveEndpoint(string vertexName, string endpointName)
        {
            var endpointInfo = await _endpointDataProvider.GetEndpoint(vertexName, endpointName);

            if (endpointInfo != null)
            { await _endpointDataProvider.DeleteEndpoint(vertexName, endpointName, endpointInfo.Value.VersionId); }
            else
            { Console.WriteLine("Could not retrieve the entity."); }
        }

        internal async Task RemoveShardedEndpoints(string vertexName, string endpointName)
        {
            var endpointInfos = await _endpointDataProvider.GetShardedEndpoints(vertexName, endpointName);

            var tasks = new List<Task>();
            foreach(var endpoint in endpointInfos)
            { tasks.Add(_endpointDataProvider.DeleteEndpoint(endpoint)); }

            await Task.WhenAll(tasks);
        }

        internal async Task<IEnumerable<string>> GetInputEndpoints(string vertexName)
            => (await _endpointDataProvider.GetEndpoints(vertexName))
            .Where(e => e.IsInput)
            .Select(e => e.EndpointName)
            .ToList();

        internal async Task<IEnumerable<string>> GetOutputEndpoints(string vertexName)
            => (await _endpointDataProvider.GetEndpoints(vertexName))
            .Where(e => !e.IsInput)
            .Select(e => e.EndpointName)
            .ToList();

        internal async Task DeleteContents()
        {
            var pendingTasks = new List<Task>();
            foreach(var ei in (await _endpointDataProvider.GetAll()))
            {
                pendingTasks.Add(
                    _endpointDataProvider.DeleteEndpoint(
                        ei.VertexName,
                        ei.EndpointName,
                        ei.VersionId));
            }

            await Task.WhenAll(pendingTasks);
        }
    }
}
