using System;
using System.Collections.Generic;
using System.Globalization;
using Microsoft.WindowsAzure.Storage.Table;

namespace CRA.ClientLibrary
{
    /// <summary>
    /// An assignment of one machine to a group
    /// </summary>
    public class EndpointTable : TableEntity
    {
        /// <summary>
        /// The time interval at which workers refresh their membership entry
        /// </summary>
        public static readonly TimeSpan HeartbeatTime = TimeSpan.FromSeconds(10);

        /// <summary>
        /// Name of the group
        /// </summary>
        public string ProcessName { get { return this.PartitionKey; } }

        /// <summary>
        /// Endpoint name
        /// </summary>
        public string EndpointName { get { return this.RowKey; } }

        /// <summary>
        /// Is an input (or output)
        /// </summary>
        public bool IsInput { get; set; }

        /// <summary>
        /// Is async (or sync)
        /// </summary>
        public bool IsAsync { get; set; }


        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="processName"></param>
        /// <param name="endpointName"></param>
        /// <param name="isInput"></param>
        /// <param name="isAsync"></param>
        public EndpointTable(string processName, string endpointName, bool isInput, bool isAsync)
        {
            this.PartitionKey = processName;
            this.RowKey = endpointName;

            this.IsInput = isInput;
            this.IsAsync = isAsync;
        }

        /// <summary>
        /// Constructor
        /// </summary>
        public EndpointTable() { }

        /// <summary>
        /// ToString
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return string.Format(CultureInfo.CurrentCulture, "Process '{0}', Endpoint '{1}'", PartitionKey, RowKey);
        }

        /// <summary>
        /// Equals
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object obj)
        {
            EndpointTable other = obj as EndpointTable;
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
        internal static IEnumerable<EndpointTable> GetAll(CloudTable instanceTable)
        {
            var query = new TableQuery<EndpointTable>();
            return instanceTable.ExecuteQuery(query);
        }
    }
}
