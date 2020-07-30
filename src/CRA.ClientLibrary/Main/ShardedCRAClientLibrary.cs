using CRA.DataProvider;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace CRA.ClientLibrary
{
    /// <summary>
    /// Client library for sharded deployment mode in Common Runtime for Applications (CRA)
    /// </summary>
    public partial class CRAClientLibrary
    {
        /// <summary>
        /// Define a sharded vertex type and register with CRA.
        /// </summary>
        /// <param name="vertexDefinition">Name of the vertex type</param>
        /// <param name="creator">Lambda that describes how to instantiate the vertex, taking in an object as parameter</param>
        public async Task<CRAErrorCode> DefineVertexAsync(
            string vertexDefinition,
            Expression<Func<IShardedVertex>> creator)
        {
            if (_artifactUploading)
            {
                using (var blobStream = await _blobStorage.GetWriteStream(vertexDefinition + "/binaries"))
                { AssemblyUtils.WriteAssembliesToStream(blobStream); }
            }
            
            // Add metadata
            var newRow = VertexInfo.Create(
                instanceName: "",
                vertexName: vertexDefinition,
                vertexDefinition: vertexDefinition,
                address: "",
                port: 0,
                vertexCreateAction: creator,
                vertexParameter: null,
                isActive: true,
                isSharded: false);

             await _vertexManager.VertexInfoProvider.InsertOrReplace(newRow);
             //await _vertexManager.VertexInfoProvider.InsertOrReplace(newRow);

            return CRAErrorCode.Success;
        }

        public async Task<ShardingInfo> GetShardingInfoAsync(string vertexName)
            => await _shardedVertexTableManager.GetLatestShardingInfo(vertexName);

        public string ShardName(string vertexName, int shardID)
        {
            return vertexName + "$" + shardID;
        }

        /// <summary>
        /// Instantiate a sharded vertex on a given CRA instance with the specified shard ID.
        /// </summary>
        /// <param name="instanceName">Name of the CRA instance</param>
        /// <param name="vertexName">Name of the sharded vertex (particular instance)</param>
        /// <param name="vertexDefinition">Definition of the vertex (type)</param>
        /// <param name="vertexParameter">Parameters to be passed to each vertex of the sharded vertex in its constructor (serializable object)</param>
        /// <param name="shardID">ID for the sharded vertex</param>
        /// <param name="shardLocator">ID for the sharded vertex</param>
        /// <returns>Status of the command</returns>
        public async Task<CRAErrorCode> InstantiateVertexAsync(
            string instanceName,
            string vertexName,
            string vertexDefinition,
            object vertexParameter,
            int shardID,
            Expression<Func<int, int>> shardLocator = null)
        {

            var addedShards = new List<int>();
            var removedShards = new List<int>();
            var vertices = await _shardedVertexTableManager.GetLatestShardedVertex(vertexName);
            if (!vertices.allInstances.Contains(instanceName))
            {
                vertices.allInstances.Add(instanceName);
            }

            if (vertices.allShards.Contains(shardID))
            {
                Console.WriteLine("Shard ID {0} already exists", shardID);
                return CRAErrorCode.VertexAlreadyExists;
            }

            vertices.allShards.Add(shardID);
            addedShards.Add(shardID);

            var task = InstantiateShardedVertexAsync
                        (instanceName, ShardName(vertexName, shardID),
                            vertexDefinition,new Tuple<int, object>(shardID, vertexParameter));

            _shardedVertexTableManager.DeleteShardedVertexAsync(vertexName).Wait();
            _shardedVertexTableManager.RegisterShardedVertexAsync(vertexName, vertices.allInstances, vertices.allShards, addedShards, removedShards, shardLocator).Wait();

            return task.Result;
        }

        /// <summary>
        /// Instantiate a sharded vertex on a set of CRA instances.
        /// </summary>
        /// <param name="vertexName">Name of the sharded vertex (particular instance)</param>
        /// <param name="vertexDefinition">Definition of the vertex (type)</param>
        /// <param name="vertexParameter">Parameters to be passed to each vertex of the sharded vertex in its constructor (serializable object)</param>
        /// <param name="vertexShards">Function mapping grain ID to shard ID</returns>
        public async Task<CRAErrorCode> InstantiateVertexAsync(
            string[] instanceNames,
            string vertexName,
            string vertexDefinition,
            object vertexParameter,
            int numShardsPerInstance = 1,
            Expression<Func<int, int>> shardLocator = null)
        {
            var allInstances = new List<string>();
            var allShards = new List<int>();
            var addedShards = new List<int>();
            var removedShards = new List<int>();


            Task<CRAErrorCode>[] tasks = new Task<CRAErrorCode>[instanceNames.Length*numShardsPerInstance];
            int index = 0;
            foreach (var instanceName in instanceNames)
            {
                for (int c = 0; c < numShardsPerInstance; c++)
                {
                    allInstances.Add(instanceName);
                    allShards.Add(index);
                    addedShards.Add(index);
                    tasks[index] = InstantiateShardedVertexAsync
                        (instanceName, ShardName(vertexName, index),
                        vertexDefinition,
                        new Tuple<int, object>(index, vertexParameter));
                    index++;
                }
            }

            _shardedVertexTableManager.DeleteShardedVertexAsync(vertexName).Wait();
            //await _shardedVertexTableManager.DeleteShardedVertex(vertexName);
            _shardedVertexTableManager.RegisterShardedVertexAsync(vertexName, allInstances, allShards, addedShards, removedShards, shardLocator).Wait();
            //await _shardedVertexTableManager.RegisterShardedVertex(vertexName, allInstances, allShards, addedShards, removedShards, shardLocator);
            
            CRAErrorCode[] results = Task.WhenAll(tasks).Result;

            // Check for the status of instantiated vertices
            CRAErrorCode result = CRAErrorCode.ConnectionEstablishFailed;
            for (int i = 0; i < results.Length; i++)
            {
                if (results[i] != CRAErrorCode.Success)
                {
                    Console.WriteLine("We received an error code " + results[i] + " from one CRA instance while instantiating the vertex " + vertexName + "$" + i + " in the sharded vertex");
                    result = results[i];
                }
                else
                    result = CRAErrorCode.Success;
            }

            if (result != CRAErrorCode.Success)
                Console.WriteLine("All CRA instances appear to be down. Restart them and this sharded vertex will be automatically instantiated");

            return result;
        }

        /// <summary>
        /// Instantiate a sharded vertex on a set of CRA instances.
        /// </summary>
        /// <param name="vertexName">Name of the sharded vertex (particular instance)</param>
        /// <param name="vertexDefinition">Definition of the vertex (type)</param>
        /// <param name="vertexParameter">Parameters to be passed to each vertex of the sharded vertex in its constructor (serializable object)</param>
        /// <param name="vertexShards"> A map that holds the number of vertices from a sharded vertex needs to be instantiated for each CRA instance </param>
        /// <returns>Status of the command</returns>
        public CRAErrorCode InstantiateShardedVertex<VertexParameterType>(string vertexName, string vertexDefinition, VertexParameterType vertexParameter, ConcurrentDictionary<string, int> vertexShards)
        {
            int verticesCount = CountVertexShards(vertexShards);
            Task<CRAErrorCode>[] tasks = new Task<CRAErrorCode>[verticesCount];
            int currentCount = 0;
            foreach (var key in vertexShards.Keys)
            {
                for (int i = currentCount; i < currentCount + vertexShards[key]; i++)
                {
                    int threadIndex = i; string currInstanceName = key;
                    tasks[threadIndex] = InstantiateShardedVertexAsync(currInstanceName, ShardName(vertexName, threadIndex),
                                                                        vertexDefinition, new Tuple<int, VertexParameterType>(threadIndex, vertexParameter));
                }

                currentCount += vertexShards[key];
            }

            CRAErrorCode[] results = Task.WhenAll(tasks).Result;

            // Check for the status of instantiated vertices
            CRAErrorCode result = CRAErrorCode.ConnectionEstablishFailed;
            for (int i = 0; i < results.Length; i++)
            {
                if (results[i] != CRAErrorCode.Success)
                {
                    Console.WriteLine("We received an error code " + results[i] + " from one CRA instance while instantiating the vertex " + vertexName + "$" + i + " in the sharded vertex");
                    result = results[i];
                }
                else
                    result = CRAErrorCode.Success;
            }

            if (result != CRAErrorCode.Success)
            {
                Console.WriteLine("All CRA instances appear to be down. Restart them and this sharded vertex will be automatically instantiated");
            }

            return result;
        }

        internal Task<CRAErrorCode> InstantiateShardedVertexAsync(
            string instanceName,
            string vertexName,
            string vertexDefinition,
            object vertexParameter)
            => InstantiateVertexAsync(
                instanceName,
                vertexName,
                vertexDefinition,
                vertexParameter,
                true);

        internal int CountVertexShards(ConcurrentDictionary<string, int> verticesPerInstanceMap)
        {
            int count = 0;
            foreach (var key in verticesPerInstanceMap.Keys)
            {
                count += verticesPerInstanceMap[key];
            }
            return count;
        }
        public bool AreTwoVerticessOnSameCRAInstance(string fromVertexName, ConcurrentDictionary<string, int> fromVertexShards, string toVertexName, ConcurrentDictionary<string, int> toVertexShards)
        {
            string fromVertexInstance = null;
            string fromVertexId = fromVertexName.Substring(fromVertexName.Length - 2);
            int fromVerticesCount = CountVertexShards(fromVertexShards);
            int currentCount = 0;
            foreach (var key in fromVertexShards.Keys)
            {
                for (int i = currentCount; i < currentCount + fromVertexShards[key]; i++)
                {
                    if (fromVertexId.Equals("$" + i))
                    {
                        fromVertexInstance = key;
                        break;
                    }
                }

                if (fromVertexInstance != null)
                    break;

                currentCount += fromVertexShards[key];
            }

            string toVertexInstance = null;
            string toVertexId = toVertexName.Substring(toVertexName.Length - 2);
            int toVerticesCount = CountVertexShards(toVertexShards);
            currentCount = 0;
            foreach (var key in toVertexShards.Keys)
            {
                for (int i = currentCount; i < currentCount + toVertexShards[key]; i++)
                {
                    if (toVertexId.Equals("$" + i))
                    {
                        toVertexInstance = key;
                        break;
                    }
                }

                if (toVertexInstance != null)
                    break;

                currentCount += toVertexShards[key];
            }

            return (fromVertexInstance != null) && (toVertexInstance != null) && (fromVertexInstance.Equals(toVertexInstance));
        }

        /// <summary>
        /// Delete all vertices in a sharded vertex from a set of CRA instances
        /// </summary>
        /// <param name="vertexName"></param>
        /// <param name="instancesNames"></param>
        public async Task DeleteShardedVertexFromInstancesAsync(string vertexName, string[] instancesNames)
        {
            Task[] tasks = new Task[instancesNames.Length];
            for (int i = 0; i < tasks.Length; i++)
            {
                int threadIndex = i;
                tasks[threadIndex] = DeleteVerticesFromInstanceAsync(vertexName, instancesNames[threadIndex]);
            }
            await Task.WhenAll(tasks);
        }

        internal async Task DeleteVerticesFromInstanceAsync(string vertexName, string instanceName)
        {
            var tasks = new List<Task>();
            foreach(var vertex in 
                await _vertexManager.VertexInfoProvider.GetRowsForShardedInstanceVertex(
                    instanceName,
                    vertexName))
            {
                tasks.Add(
                    _vertexManager.VertexInfoProvider.DeleteVertexInfo(vertex));
            }

            await Task.WhenAll(tasks);
        }

        private void FilterAndDeleteEntitiesInBatches(
            CloudTable table,
            Expression<Func<DynamicTableEntity, bool>> filter)
        {
            Action<IEnumerable<DynamicTableEntity>> deleteProcessor = entities =>
            {
                var batches = new Dictionary<string, TableBatchOperation>();
                foreach (var entity in entities)
                {
                    TableBatchOperation batch = null;
                    if (batches.TryGetValue(entity.PartitionKey, out batch) == false)
                    {
                        batches[entity.PartitionKey] = batch = new TableBatchOperation();
                    }

                    batch.Add(TableOperation.Delete(entity));

                    if (batch.Count == 100)
                    {
                        table.ExecuteBatchAsync(batch).Wait();
                        batches[entity.PartitionKey] = new TableBatchOperation();
                    }
                }

                foreach (var batch in batches.Values)
                {
                    if (batch.Count > 0)
                    {
                        table.ExecuteBatchAsync(batch).Wait();
                    }
                }
            };

            FilterAndVertexEntitiesInSegments(table, deleteProcessor, filter);
        }


        private void FilterAndVertexEntitiesInSegments(CloudTable table, Action<IEnumerable<DynamicTableEntity>> operationExecutor, Expression<Func<DynamicTableEntity, bool>> filter)
        {
            if (filter == null)
            {
                operationExecutor(table.ExecuteQuery(new TableQuery<DynamicTableEntity>()));
            }
            else
            {
                operationExecutor(table.ExecuteQuery(new TableQuery<DynamicTableEntity>()).Where(filter.Compile()));
            }
        }
         
        /// <summary>
        /// Delete an endpoint from all vertices in a sharded vertex
        /// </summary>
        /// <param name="vertexName"></param>
        /// <param name="endpointName"></param>
        public void DeleteEndpointFromShardedVertex(string vertexName, string endpointName)
        {
            DeleteEndpointsFromAllVertices(vertexName, endpointName);
        }

        private Task DeleteEndpointsFromAllVertices(string vertexName, string endpointName)
            => _endpointTableManager.RemoveShardedEndpoints(vertexName, endpointName);

        /// <summary>
        /// Connect a set of (fromVertex, outputEndpoint) pairs to another set of (toVertex, inputEndpoint) pairs. We contact the "from" vertex
        /// to initiate the creation of the link.
        /// </summary>
        /// <param name="fromToConnections"></param>
        /// <returns></returns>
        public CRAErrorCode ConnectShardedVertices(List<ConnectionInfoWithLocality> fromToConnections)
        {
            Task<CRAErrorCode>[] tasks = new Task<CRAErrorCode>[fromToConnections.Count];

            int i = 0;
            foreach (var fromToConnection in fromToConnections)
            {
                int fromToEndpointIndex = i;
                var currentFromToConnection = fromToConnection;
                tasks[fromToEndpointIndex] = ConnectAsync(currentFromToConnection.FromVertex,
                                                currentFromToConnection.FromEndpoint, currentFromToConnection.ToVertex,
                                                currentFromToConnection.ToEndpoint, ConnectionInitiator.FromSide);
                i++;
            }
            CRAErrorCode[] results = Task.WhenAll(tasks).Result;

            CRAErrorCode result = CRAErrorCode.VertexEndpointNotFound;
            for (i = 0; i < results.Length; i++)
            {
                if (results[i] != CRAErrorCode.Success)
                {
                    Console.WriteLine("We received an error code " + results[i] + " while vertexing the connection pair with number (" + i + ")");
                    result = results[i];
                }
                else
                    result = CRAErrorCode.Success;
            }

            if (result != CRAErrorCode.Success)
                Console.WriteLine("All CRA instances appear to be down. Restart them and connect these two sharded vertices again!");

            return result;
        }


        /// <summary>
        /// Connect a set of vertices in one sharded CRA vertex to another with a full mesh topology, via pre-defined endpoints. We contact the "from" vertex
        /// to initiate the creation of the link.
        /// </summary>
        /// <param name="fromVertexName"></param>
        /// <param name="fromEndpoints"></param>
        /// <param name="toVertexName"></param>
        /// <param name="toEndpoints"></param>
        /// <returns></returns>
        public async Task<CRAErrorCode> ConnectShardedVerticesWithFullMesh(string fromVertexName, string[] fromEndpoints, string toVertexName, string[] toEndpoints)
        {
            var fromVerticesRows = await _vertexManager
                .VertexInfoProvider
                .GetRowsForShardedVertex(fromVertexName);

            string[] fromVerticesNames = new string[fromVerticesRows.Count()];

            int i = 0;
            foreach (var fromVertexRow in fromVerticesRows)
            {
                fromVerticesNames[i] = fromVertexRow.VertexName;
                i++;
            }

            var toVerticesRows = await _vertexManager
                .VertexInfoProvider
                .GetRowsForShardedVertex(toVertexName);

            string[] toVerticesNames = new string[toVerticesRows.Count()];

            i = 0;
            foreach (var toVertexRow in toVerticesRows)
            {
                toVerticesNames[i] = toVertexRow.VertexName;
                i++;
            }

            return ConnectShardedVerticesWithFullMesh(
                fromVerticesNames,
                fromEndpoints,
                toVerticesNames,
                toEndpoints,
                ConnectionInitiator.FromSide);
        }

        /// <summary>
        /// Connect a set of vertices in one sharded CRA vertex to another with a full mesh toplogy, via pre-defined endpoints. We contact the "from" vertex
        /// to initiate the creation of the link.
        /// </summary>
        /// <param name="fromVerticesNames"></param>
        /// <param name="fromEndpoints"></param>
        /// <param name="toVerticesNames"></param>
        /// <param name="toEndpoints"></param>
        /// <param name="direction"></param>
        /// <returns></returns>
        public CRAErrorCode ConnectShardedVerticesWithFullMesh(
            string[] fromVerticesNames,
            string[] fromEndpoints,
            string[] toVerticesNames,
            string[] toEndpoints,
            ConnectionInitiator direction)
        {
            // Check if the number of output endpoints in any fromVertex is equal to 
            // the number of toVertices
            if (fromEndpoints.Length != toVerticesNames.Length)
                return CRAErrorCode.VerticesEndpointsNotMatched;

            // Check if the number of input endpoints in any toVertex is equal to 
            // the number of fromVertices
            if (toEndpoints.Length != fromVerticesNames.Length)
                return CRAErrorCode.VerticesEndpointsNotMatched;

            //Connect the sharded vertices in parallel
            List<Task<CRAErrorCode>> tasks = new List<Task<CRAErrorCode>>();
            for (int i = 0; i < fromEndpoints.Length; i++)
            {
                for (int j = 0; j < fromVerticesNames.Length; j++)
                {
                    int fromEndpointIndex = i;
                    int fromVertexIndex = j;
                    tasks.Add(ConnectAsync(fromVerticesNames[fromVertexIndex], fromEndpoints[fromEndpointIndex],
                                           toVerticesNames[fromEndpointIndex], toEndpoints[fromVertexIndex], direction));
                }
            }
            CRAErrorCode[] results = Task.WhenAll(tasks.ToArray()).Result;

            //Check for the status of connected vertices
            CRAErrorCode result = CRAErrorCode.VertexEndpointNotFound;
            for (int i = 0; i < results.Length; i++)
            {
                if (results[i] != CRAErrorCode.Success)
                {
                    Console.WriteLine("We received an error code " + results[i] + " while vertexing the connection pair with number (" + i + ")");
                    result = results[i];
                }
                else
                    result = CRAErrorCode.Success;
            }

            if (result != CRAErrorCode.Success)
                Console.WriteLine("All CRA instances appear to be down. Restart them and connect these two sharded vertices again!");

            return result;
        }

        /// <summary>
        /// Disconnect a set of vertices in one sharded CRA vertex from another, via pre-defined endpoints. 
        /// </summary>
        /// <param name="fromVertexName"></param>
        /// <param name="fromEndpoints"></param>
        /// <param name="toVertexName"></param>
        /// <param name="toEndpoints"></param>
        /// <returns></returns>
        public async Task DisconnectShardedVertices(string fromVertexName, string[] fromEndpoints, string toVertexName, string[] toEndpoints)
        {
            var fromVerticesRows = await _vertexManager
                .VertexInfoProvider
                .GetRowsForShardedVertex(fromVertexName);
            string[] fromVerticesNames = new string[fromVerticesRows.Count()];

            int i = 0;
            foreach (var fromVertexRow in fromVerticesRows)
            {
                fromVerticesNames[i] = fromVertexRow.VertexName;
                i++;
            }

            var toVerticesRows = await _vertexManager
                .VertexInfoProvider
                .GetRowsForShardedVertex(toVertexName);

            string[] toVerticesNames = new string[toVerticesRows.Count()];

            i = 0;
            foreach (var toVertexRow in toVerticesRows)
            {
                toVerticesNames[i] = toVertexRow.VertexName;
                i++;
            }

            await DisconnectShardedVertices(
                fromVerticesNames,
                fromEndpoints,
                toVerticesNames,
                toEndpoints);
        }

        /// <summary>
        /// Disconnect the CRA connections between vertices in two sharded vertices
        /// </summary>
        /// <param name="fromVerticesNames"></param>
        /// <param name="fromVerticesOutputs"></param>
        /// <param name="toVerticesNames"></param>
        /// <param name="toVerticesInputs"></param>
        public async Task DisconnectShardedVertices(
            string[] fromVerticesNames,
            string[] fromVerticesOutputs,
            string[] toVerticesNames,
            string[] toVerticesInputs)
        {
            // Check if the number of output endpoints in any fromVertex is equal to 
            // the number of toVertices, and the number of input endpoints in any toVertex 
            // is equal to the number of fromVertices
            if ((fromVerticesOutputs.Length == toVerticesNames.Length) && (toVerticesInputs.Length == fromVerticesNames.Length))
            {
                //Disconnect the sharded vertices in parallel
                List<Task> tasks = new List<Task>();
                for (int i = 0; i < fromVerticesOutputs.Length; i++)
                {
                    for (int j = 0; j < fromVerticesNames.Length; j++)
                    {
                        int fromEndpointIndex = i;
                        int fromVertexIndex = j;
                        tasks.Add(
                            DisconnectAsync(
                                fromVerticesNames[fromVertexIndex],
                                fromVerticesOutputs[fromEndpointIndex],
                                toVerticesNames[fromEndpointIndex],
                                toVerticesInputs[fromVertexIndex]));
                    }
                }

                await Task.WhenAll(tasks);
            }
        }
    }
}
