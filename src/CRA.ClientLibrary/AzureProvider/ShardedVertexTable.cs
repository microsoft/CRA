using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using Microsoft.WindowsAzure.Storage.Table;
using CRA.DataProvider;

namespace CRA.ClientLibrary
{
    /// <summary>
    /// An assignment of one machine to a group
    /// </summary>
    public class ShardedVertexTable : TableEntity
    {

        /// <summary>
        /// Name of the CRA instance
        /// </summary>
        public string VertexName { get { return this.PartitionKey; } }

        /// <summary>
        /// Name of vertex
        /// </summary>
        public string EpochId { get { return this.RowKey; } }

        /// <summary>
        /// Instances for the vertex
        /// </summary>
        public string AllInstances { get; set; }

        public string AllShards { get; set; }
        public string AddedShards { get; set; }
        public string RemovedShards { get; set; }

        /// <summary>
        /// Expression to locate a shard in the new sharding scheme
        /// </summary>
        public string ShardLocator { get; set; }


        public ShardedVertexTable(string vertexName, string epochId,
            List<string> allInstances,
            List<int> allShards, List<int> addedShards, List<int> removedShards,
            Expression<Func<int, int>> shardLocator)
        {
            this.PartitionKey = vertexName;
            this.RowKey = epochId;

            this.AllInstances = string.Join(";", allInstances);
            this.AllShards = string.Join(";", allShards);
            this.AddedShards = string.Join(";", addedShards);
            this.RemovedShards = string.Join(";", removedShards);
            this.ShardLocator = "";

            if (shardLocator != null)
            {
                var closureEliminator = new ClosureEliminator();
                Expression vertexedUserLambdaExpression = closureEliminator.Visit(shardLocator);
                this.ShardLocator = SerializationHelper.Serialize(vertexedUserLambdaExpression);
            }
        }


        /// <summary>
        /// Constructor
        /// </summary>
        public ShardedVertexTable() { }

        public static implicit operator ShardedVertexTable(ShardedVertexInfo vertexInfo)
            => new ShardedVertexTable
            {
                PartitionKey = vertexInfo.VertexName,
                RowKey = vertexInfo.EpochId,
                AllInstances = vertexInfo.AllInstances,
                AllShards = vertexInfo.AllShards,
                AddedShards = vertexInfo.AddedShards,
                RemovedShards = vertexInfo.RemovedShards,
                ShardLocator = vertexInfo.ShardLocator,
                ETag = vertexInfo.VersionId
            };

        public static implicit operator ShardedVertexInfo(ShardedVertexTable vertexInfo)
            => new ShardedVertexInfo(
                vertexName: vertexInfo.VertexName,
                epochId: vertexInfo.EpochId,
                allInstances: vertexInfo.AllInstances,
                allShards: vertexInfo.AllShards,
                addedShards: vertexInfo.AddedShards,
                removedShards: vertexInfo.RemovedShards,
                shardLocator: vertexInfo.ShardLocator,
                versionId: vertexInfo.ETag);

        /// <summary>
        /// ToString
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return string.Format(CultureInfo.CurrentCulture, "Vertex '{0}', EpochId '{1}'", this.VertexName, this.EpochId);
        }

        /// <summary>
        /// Equals
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object obj)
        {
            ShardedVertexTable other = obj as ShardedVertexTable;
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

        internal Func<int, int> GetShardLocator()
        {
            var expr = SerializationHelper.Deserialize(ShardLocator);
            var actionExpr = (Expression<Func<int, int>>)expr;
            return actionExpr.Compile();
        }

        internal Expression<Func<int, int>> GetShardLocatorExpr()
        {
            var expr = SerializationHelper.Deserialize(ShardLocator);
            return (Expression<Func<int, int>>) expr;
        }

        /// <summary>
        /// Returns a list of all visible nodes in all groups
        /// </summary>
        /// <param name="instanceTable"></param>
        /// <returns></returns>
        internal static IEnumerable<ShardedVertexTable> GetAll(CloudTable instanceTable)
        {
            TableQuery<ShardedVertexTable> query = new TableQuery<ShardedVertexTable>();
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

        internal static ShardedVertexTable GetEntryForVertex(CloudTable instanceTable, string vertexName, string epochId)
        {
            return GetAll(instanceTable).Where(gn => vertexName == gn.VertexName && epochId == gn.EpochId).First();
        }

        internal static IEnumerable<ShardedVertexTable> GetEntriesForVertex(CloudTable instanceTable, string vertexName)
        {
            return GetAll(instanceTable).Where(gn => vertexName == gn.VertexName);
        }

        internal static ShardedVertexTable GetLatestEntryForVertex(CloudTable instanceTable, string vertexName)
        {
            return GetAll(instanceTable).Where(gn => vertexName == gn.VertexName)
                .OrderByDescending(e => Int32.Parse(e.EpochId)).First();
        }
    }
}
