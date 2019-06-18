namespace CRA.DataProvider
{
    using System;
    using System.Collections.Generic;
    using System.Linq.Expressions;
    using CRA.ClientLibrary;

    /// <summary>
    /// Definition for ShardedVertexInfo
    /// </summary>
    public struct ShardedVertexInfo
    {
        public ShardedVertexInfo(
            string vertexName,
            string epochId,
            string allInstances,
            string allShards,
            string addedShards,
            string removedShards,
            string shardLocator,
            string versionId = null)
        {
            this.VertexName = vertexName;
            this.EpochId = epochId;
            this.AllInstances = allInstances;
            this.AllShards = allShards;
            this.AddedShards = addedShards;
            this.RemovedShards = removedShards;
            this.ShardLocator = shardLocator;
            this.VersionId = versionId;
        }

        public static ShardedVertexInfo Create(
            string vertexName,
            string epochId,
            List<string> allInstances,
            List<int> allShards,
            List<int> addedShards,
            List<int> removedShards,
            Expression<Func<int, int>> shardLocator)
        {
            var strAllInstances = string.Join(";", allInstances);
            var strAllShards = string.Join(";", allShards);
            var strAddedShards = string.Join(";", addedShards);
            var strRemovedShards = string.Join(";", removedShards);
            var strShardLocator = "";

            if (shardLocator != null)
            {
                var closureEliminator = new ClosureEliminator();
                Expression vertexedUserLambdaExpression = closureEliminator.Visit(shardLocator);
                strShardLocator = SerializationHelper.Serialize(vertexedUserLambdaExpression);
            }

            return new ShardedVertexInfo(
                vertexName: vertexName,
                epochId: epochId,
                allInstances: strAllInstances,
                allShards: strAllShards,
                addedShards: strAddedShards,
                removedShards: strRemovedShards,
                shardLocator: strShardLocator);
        }
        

        public string VertexName { get; }
        public string EpochId { get; }
        public string AllInstances { get; }
        public string AllShards { get; }
        public string AddedShards { get; }
        public string RemovedShards { get; }
        public string ShardLocator { get; }
        public string VersionId { get; }

        public override string ToString()
            => string.Format("Vertex '{0}', EpochId '{1}'", this.VertexName, this.EpochId);

        public override bool Equals(object obj)
        {
            switch (obj)
            {
                case ShardedVertexInfo other:
                    return other.VertexName == this.VertexName && other.EpochId == this.EpochId;
            }

            return false;
        }

        public override int GetHashCode()
            => this.VertexName.GetHashCode() ^ this.EpochId.GetHashCode();

        public static bool operator ==(ShardedVertexInfo left, ShardedVertexInfo right)
            => left.VertexName == right.VertexName && left.EpochId == right.EpochId;

        public static bool operator !=(ShardedVertexInfo left, ShardedVertexInfo right)
            => !(left == right);
    }
}
