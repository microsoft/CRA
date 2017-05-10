using Microsoft.WindowsAzure.Storage.Table;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace CRA.ClientLibrary
{
    /// <summary>
    /// An assignment of one machine to a group
    /// </summary>
    public class ConnectionTable : TableEntity
    {
        /// <summary>
        /// Name of the from process
        /// </summary>
        public string FromProcess { get { return this.PartitionKey; } }

        /// <summary>
        /// Other data related to connection
        /// </summary>
        public string EndpointToProcessEndpoint { get { return this.RowKey; } }

        /// <summary>
        /// From endpoint
        /// </summary>
        public string FromEndpoint { get { return EndpointToProcessEndpoint.Split(':')[0]; } }

        /// <summary>
        /// To process
        /// </summary>
        public string ToProcess { get { return EndpointToProcessEndpoint.Split(':')[1]; } }

        /// <summary>
        /// To endpoint
        /// </summary>
        public string ToEndpoint { get { return EndpointToProcessEndpoint.Split(':')[2]; } }

        /// <summary>
        /// Connection table
        /// </summary>
        /// <param name="fromProcess"></param>
        /// <param name="fromEndpoint"></param>
        /// <param name="toProcess"></param>
        /// <param name="toEndpoint"></param>
        public ConnectionTable(string fromProcess, string fromEndpoint, string toProcess, string toEndpoint)
        {
            this.PartitionKey = fromProcess;
            this.RowKey = fromEndpoint + ":" + toProcess + ":" + toEndpoint;
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
            return string.Format(CultureInfo.CurrentCulture, "FromProcess '{0}', FromEndpoint '{1}', ToProcess '{2}', ToEndpoint '{3}'", FromProcess, FromEndpoint, ToProcess, ToEndpoint);
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

        /// <summary>
        /// Counts all nodes in the cluster regardless of their group
        /// </summary>
        /// <returns></returns>
        internal static int CountAll(CloudTable instanceTable)
        {
            return GetAll(instanceTable).Count();
        }

        internal static IEnumerable<ConnectionTable> GetAllConnectionsFromProcess(CloudTable instanceTable, string fromProcess)
        {
            return GetAll(instanceTable).Where(gn => fromProcess == gn.PartitionKey);
        }

        internal static IEnumerable<ConnectionTable> GetAllConnectionsToProcess(CloudTable instanceTable, string toProcess)
        {
            return GetAll(instanceTable).Where(gn => toProcess == gn.ToProcess);
        }

        internal static bool ContainsConnection(CloudTable instanceTable, string fromProcess, string fromEndpoint, string toProcess, string toEndpoint)
        {
            return ContainsRow(instanceTable, new ConnectionTable(fromProcess, fromEndpoint, toProcess, toEndpoint));
        }

        internal static bool ContainsRow(CloudTable instanceTable, ConnectionTable entity)
        {
            var temp = GetAll(instanceTable);

            return temp.Where(gn => entity.Equals(gn)).Count() > 0;
        }
    }
}
