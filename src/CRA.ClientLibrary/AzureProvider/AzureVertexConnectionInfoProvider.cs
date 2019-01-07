//-----------------------------------------------------------------------
// <copyright file="AzureVertexConnectionInfoProvider.cs" company="">
//     Copyright (c) . All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

namespace CRA.ClientLibrary.AzureProvider
{
    using CRA.ClientLibrary.DataProvider;
    using Microsoft.WindowsAzure.Storage.Table;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;

    public class AzureVertexConnectionInfoProvider
        : IVertexConnectionInfoProvider
    {
        private CloudTable _cloudTable;

        public AzureVertexConnectionInfoProvider(CloudTable cloudTable)
        { _cloudTable = cloudTable; }

        public async Task<bool> ContainsRow(VertexConnectionInfo entity)
        {
            var temp = await GetAll();

            return temp
                .Where(gn => entity.Equals(gn))
                .Count() > 0;
        }

        public async Task<int> CountAll()
        {
            var query = new TableQuery<ConnectionTable>();
            return (await _cloudTable.ExecuteQueryAsync(query)).Count();
        }

        public async Task<IEnumerable<VertexConnectionInfo>> GetAll()
        {
            var query = new TableQuery<ConnectionTable>();
            return (await _cloudTable.ExecuteQueryAsync(query))
                .Select(_ =>
                    new VertexConnectionInfo
                    {
                        FromEndpoint = _.FromEndpoint,
                        FromVertex = _.FromVertex,
                        ToEndpoint = _.ToEndpoint,
                        ToVertex = _.ToVertex
                    });
        }

        public async Task<IEnumerable<VertexConnectionInfo>> GetAllConnectionsFromVertex(string fromVertex)
        {
            return (await GetAll())
                .Where(gn => fromVertex == gn.FromVertex);
        }

        public async Task<IEnumerable<VertexConnectionInfo>> GetAllConnectionsToVertex(string toVertex)
        {
            return (await GetAll())
                .Where(gn => toVertex == gn.ToVertex);
        }
    }
}
