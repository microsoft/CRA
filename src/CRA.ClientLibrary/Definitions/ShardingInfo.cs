using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Runtime.Serialization;

namespace CRA.ClientLibrary
{
    /// <summary>
    /// Describes the sharding configuration
    /// </summary>
    public class ShardingInfo
    {
        /// <summary>
        /// List of all shards
        /// </summary>
        public int[] AllShards { get; set; }

        /// <summary>
        /// Shards added most recently
        /// </summary>
        public int[] AddedShards { get; set; }

        /// <summary>
        /// Shards removed most recently
        /// </summary>
        public int[] RemovedShards { get; set; }

        /// <summary>
        /// Map of key to offset in AllShards array
        /// </summary>
        public Expression<Func<int, int>> ShardLocator { get; set; }
    }
}
