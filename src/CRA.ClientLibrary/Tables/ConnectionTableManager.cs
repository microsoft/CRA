using System.Collections.Generic;
using CRA.DataProvider;
using System.Threading.Tasks;

namespace CRA.ClientLibrary
{
    /// <summary>
    /// An assignment of one machine to a group
    /// </summary>
    public class ConnectionTableManager
    {
        private IVertexConnectionInfoProvider _connectionProvider;

        internal ConnectionTableManager(IDataProvider dataProvider)
        {
            _connectionProvider = dataProvider.GetVertexConnectionInfoProvider();
        }

        internal Task DeleteTable()
            => _connectionProvider.DeleteStore();


        internal Task AddConnection(string fromVertex, string fromOutput, string toConnection, string toInput)
            => _connectionProvider.Add(
                new VertexConnectionInfo(
                    fromVertex: fromVertex,
                    fromEndpoint: fromOutput,
                    toVertex: toConnection,
                    toEndpoint: toInput));

        internal async Task DeleteConnection(string fromVertex, string fromOutput, string toConnection, string toInput)
        {
            var vci = await _connectionProvider.Get(fromVertex, fromOutput, toConnection, toInput);

            if (vci != null)
            { await _connectionProvider.Delete(vci.Value); }
        }

        internal Task<IEnumerable<VertexConnectionInfo>> GetConnectionsFromVertex(string vertexName)
            => _connectionProvider.GetAllConnectionsFromVertex(vertexName);

        internal Task<IEnumerable<VertexConnectionInfo>> GetConnectionsToVertex(string vertexName)
            => _connectionProvider.GetAllConnectionsToVertex(vertexName);
    }
}
