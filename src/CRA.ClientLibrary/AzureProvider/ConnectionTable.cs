using CRA.ClientLibrary.DataProvider;
using Microsoft.WindowsAzure.Storage.Table;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace CRA.ClientLibrary.AzureProvider
{
    /// <summary>
    /// An assignment of one machine to a group
    /// </summary>
    public class ConnectionTable : TableEntity
    {
        /// <summary>
        /// Name of the from vertex
        /// </summary>
        public string FromVertex { get { return this.PartitionKey; } }

        /// <summary>
        /// Other data related to connection
        /// </summary>
        public string EndpointToVertexEndpoint { get { return this.RowKey; } }

        /// <summary>
        /// From endpoint
        /// </summary>
        public string FromEndpoint { get { return EndpointToVertexEndpoint.Split(':')[0]; } }

        /// <summary>
        /// To vertex
        /// </summary>
        public string ToVertex { get { return EndpointToVertexEndpoint.Split(':')[1]; } }

        /// <summary>
        /// To endpoint
        /// </summary>
        public string ToEndpoint { get { return EndpointToVertexEndpoint.Split(':')[2]; } }

        /// <summary>
        /// Connection table
        /// </summary>
        /// <param name="fromVertex"></param>
        /// <param name="fromEndpoint"></param>
        /// <param name="toVertex"></param>
        /// <param name="toEndpoint"></param>
        public ConnectionTable(string fromVertex, string fromEndpoint, string toVertex, string toEndpoint)
        {
            this.PartitionKey = fromVertex;
            this.RowKey = fromEndpoint + ":" + toVertex + ":" + toEndpoint;
        }

        /// <summary>
        /// 
        /// </summary>
        public ConnectionTable() { }

        /// <summary>
        /// ToString
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return string.Format(CultureInfo.CurrentCulture, "FromVertex '{0}', FromEndpoint '{1}', ToVertex '{2}', ToEndpoint '{3}'", FromVertex, FromEndpoint, ToVertex, ToEndpoint);
        }

        /// <summary>
        /// Equality
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object obj)
        {
            ConnectionTable other = obj as ConnectionTable;
            return this.PartitionKey.Equals(other.PartitionKey) && this.RowKey.Equals(other.RowKey);
        }


        /// <summary>
        /// GetHashCode
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            return PartitionKey.GetHashCode() ^ RowKey.GetHashCode();
        }

        /// <summary>
        /// Returns a list of all visible nodes in all groups
        /// </summary>
        /// <param name="instanceTable"></param>
        /// <returns></returns>
        internal static IEnumerable<ConnectionTable> GetAll(CloudTable instanceTable)
        {
            TableQuery<ConnectionTable> query = new TableQuery<ConnectionTable>();
            return instanceTable.ExecuteQuery(query);
        }

        public static implicit operator VertexConnectionInfo(ConnectionTable ct)
            => new VertexConnectionInfo(
                fromVertex: ct.FromVertex,
                fromEndpoint: ct.FromEndpoint,
                toVertex: ct.ToVertex,
                toEndpoint: ct.ToEndpoint,
                versionId: ct.ETag);

        public static implicit operator ConnectionTable(VertexConnectionInfo vci)
            => new ConnectionTable(
                fromVertex: vci.FromVertex,
                fromEndpoint: vci.FromEndpoint,
                toVertex: vci.ToVertex,
                toEndpoint: vci.ToEndpoint)
            { ETag = vci.VersionId };

        /// <summary>
        /// Counts all nodes in the cluster regardless of their group
        /// </summary>
        /// <returns></returns>
        internal static int CountAll(CloudTable instanceTable)
        {
            return GetAll(instanceTable).Count();
        }

        internal static IEnumerable<ConnectionTable> GetAllConnectionsFromVertex(CloudTable instanceTable, string fromVertex)
        {
            return GetAll(instanceTable).Where(gn => fromVertex == gn.PartitionKey);
        }

        internal static IEnumerable<ConnectionTable> GetAllConnectionsToVertex(CloudTable instanceTable, string toVertex)
        {
            return GetAll(instanceTable).Where(gn => toVertex == gn.ToVertex);
        }

        internal static bool ContainsConnection(CloudTable instanceTable, string fromVertex, string fromEndpoint, string toVertex, string toEndpoint)
        {
            return ContainsRow(instanceTable, new ConnectionTable(fromVertex, fromEndpoint, toVertex, toEndpoint));
        }

        internal static bool ContainsRow(CloudTable instanceTable, ConnectionTable entity)
        {
            var temp = GetAll(instanceTable);

            return temp.Where(gn => entity.Equals(gn)).Count() > 0;
        }
    }
}
