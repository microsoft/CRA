using System;
using System.Collections.Generic;
using System.Globalization;
using CRA.ClientLibrary.DataProvider;
using Microsoft.WindowsAzure.Storage.Table;

namespace CRA.ClientLibrary.AzureProvider
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
        public string VertexName { get { return this.PartitionKey; } }

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
        /// <param name="vertexName"></param>
        /// <param name="endpointName"></param>
        /// <param name="isInput"></param>
        /// <param name="isAsync"></param>
        public EndpointTable(string vertexName, string endpointName, bool isInput, bool isAsync)
        {
            this.PartitionKey = vertexName;
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
            return string.Format(CultureInfo.CurrentCulture, "Vertex '{0}', Endpoint '{1}'", PartitionKey, RowKey);
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

        public static implicit operator EndpointInfo(EndpointTable et)
            => new EndpointInfo(
                vertexName: et.VertexName,
                endpointName: et.EndpointName,
                isInput: et.IsInput,
                isAsync: et.IsAsync,
                versionId: et.ETag);

        public static implicit operator EndpointTable(EndpointInfo ei)
            => new EndpointTable(
                vertexName: ei.VertexName,
                endpointName: ei.EndpointName,
                isInput: ei.IsInput,
                isAsync: ei.IsAsync)
            { ETag = ei.VersionId };
    }
}
