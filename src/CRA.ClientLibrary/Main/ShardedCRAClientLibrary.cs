using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage.Table.Queryable;
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
        /// Instantiate a sharded vertex on a set of CRA instances.
        /// </summary>
        /// <param name="vertexName">Name of the sharded vertex (particular instance)</param>
        /// <param name="vertexDefinition">Definition of the vertex (type)</param>
        /// <param name="vertexParameter">Parameters to be passed to each vertex of the sharded vertex in its constructor (serializable object)</param>
        /// <param name="vertexShards"> A map that holds the number of vertexes from a sharded vertex needs to be instantiated for each CRA instance </param>
        /// <returns>Status of the command</returns>
        public CRAErrorCode InstantiateShardedVertex<VertexParameterType>(string vertexName, string vertexDefinition, VertexParameterType vertexParameter, ConcurrentDictionary<string, int> vertexShards)
        {
            int vertexesCount = CountVertexShards(vertexShards);
            Task<CRAErrorCode>[] tasks = new Task<CRAErrorCode>[vertexesCount];
            int currentCount = 0;
            foreach (var key in vertexShards.Keys)
            {
                for (int i = currentCount; i < currentCount + vertexShards[key]; i++)
                {
                    int threadIndex = i; string currInstanceName = key;
                    tasks[threadIndex] = InstantiateVertexAsync(currInstanceName, vertexName + "$" + threadIndex,
                                                                        vertexDefinition, new Tuple<int, VertexParameterType>(threadIndex, vertexParameter));
                }
                currentCount += vertexShards[key];
            }
            CRAErrorCode[] results = Task.WhenAll(tasks).Result;

            // Check for the status of instantiated vertexes
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

        internal Task<CRAErrorCode> InstantiateVertexAsync(string instanceName, string vertexName, string vertexDefinition, object vertexParameter)
        {
            return Task.Factory.StartNew(
                () => { return InstantiateVertex(instanceName, vertexName, vertexDefinition, vertexParameter); });
        }

        internal int CountVertexShards(ConcurrentDictionary<string, int> vertexesPerInstanceMap)
        {
            int count = 0;
            foreach (var key in vertexesPerInstanceMap.Keys)
            {
                count += vertexesPerInstanceMap[key];
            }
            return count;
        }
        public bool AreTwoVertexessOnSameCRAInstance(string fromVertexName, ConcurrentDictionary<string, int> fromVertexShards, string toVertexName, ConcurrentDictionary<string, int> toVertexShards)
        {
            string fromVertexInstance = null;
            string fromVertexId = fromVertexName.Substring(fromVertexName.Length - 2);
            int fromVertexesCount = CountVertexShards(fromVertexShards);
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
            int toVertexesCount = CountVertexShards(toVertexShards);
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
        /// Delete all vertexes in a sharded vertex from a set of CRA instances
        /// </summary>
        /// <param name="vertexName"></param>
        /// <param name="instancesNames"></param>
        public void DeleteShardedVertexFromInstances(string vertexName, string[] instancesNames)
        {
            Task[] tasks = new Task[instancesNames.Length];
            for (int i = 0; i < tasks.Length; i++)
            {
                int threadIndex = i;
                tasks[threadIndex] = DeleteVertexesFromInstanceAsync(vertexName, instancesNames[threadIndex]);
            }
            Task.WhenAll(tasks);
        }

        internal Task DeleteVertexesFromInstanceAsync(string vertexName, string instanceName)
        {
            return Task.Factory.StartNew(
                () => { DeleteVertexesFromInstance(vertexName, instanceName); });
        }

        private void DeleteVertexesFromInstance(string vertexName, string instanceName)
        {
            Expression<Func<DynamicTableEntity, bool>> vertexesFilter = (e => e.PartitionKey == instanceName && e.RowKey.StartsWith(vertexName + "$"));
            FilterAndDeleteEntitiesInBatches(_vertexTable, vertexesFilter);
        }

        private void FilterAndDeleteEntitiesInBatches(CloudTable table, Expression<Func<DynamicTableEntity, bool>> filter)
        {
            Action<IEnumerable<DynamicTableEntity>> deleteVertexor = entities =>
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
                        table.ExecuteBatch(batch);
                        batches[entity.PartitionKey] = new TableBatchOperation();
                    }
                }

                foreach (var batch in batches.Values)
                {
                    if (batch.Count > 0)
                    {
                        table.ExecuteBatch(batch);
                    }
                }
            };

            FilterAndVertexEntitiesInSegments(table, deleteVertexor, filter);
        }


        private void FilterAndVertexEntitiesInSegments(CloudTable table, Action<IEnumerable<DynamicTableEntity>> operationExecutor, Expression<Func<DynamicTableEntity, bool>> filter)
        {
            TableQuerySegment<DynamicTableEntity> segment = null;
            while (segment == null || segment.ContinuationToken != null)
            {
                if (filter == null)
                {
                    segment = table.ExecuteQuerySegmented(new TableQuery().Take(100), segment == null ? null : segment.ContinuationToken);
                }
                else
                {
                    var query = table.CreateQuery<DynamicTableEntity>().Where(filter).Take(100).AsTableQuery();
                    segment = query.ExecuteSegmented(segment == null ? null : segment.ContinuationToken);
                }

                operationExecutor(segment.Results);
            }
        }

        /// <summary>
        /// Delete an endpoint from all vertexes in a sharded vertex
        /// </summary>
        /// <param name="vertexName"></param>
        /// <param name="endpointName"></param>
        public void DeleteEndpointFromShardedVertex(string vertexName, string endpointName)
        {
            DeleteEndpointsFromAllVertexes(vertexName, endpointName);
        }

        private void DeleteEndpointsFromAllVertexes(string vertexName, string endpointName)
        {
            Expression<Func<DynamicTableEntity, bool>> filters = (e => e.PartitionKey.StartsWith(vertexName + "$") && e.RowKey == endpointName);
            FilterAndDeleteEntitiesInBatches(_endpointTableManager.EndpointTable, filters);
        }

        /// <summary>
        /// Connect a set of (fromVertex, outputEndpoint) pairs to another set of (toVertex, inputEndpoint) pairs. We contact the "from" vertex
        /// to initiate the creation of the link.
        /// </summary>
        /// <param name="fromToConnections"></param>
        /// <returns></returns>
        public CRAErrorCode ConnectShardedVertexes(List<ConnectionInfoWithLocality> fromToConnections)
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
                Console.WriteLine("All CRA instances appear to be down. Restart them and connect these two sharded vertexes again!");

            return result;
        }

        internal Task<CRAErrorCode> ConnectAsync(string fromVertexName, string fromEndpoint, string toVertexName, string toEndpoint, ConnectionInitiator direction)
        {
            return Task.Factory.StartNew(
                () => { return Connect(fromVertexName, fromEndpoint, toVertexName, toEndpoint, direction); });
        }


        /// <summary>
        /// Connect a set of vertexes in one sharded CRA vertex to another with a full mesh topology, via pre-defined endpoints. We contact the "from" vertex
        /// to initiate the creation of the link.
        /// </summary>
        /// <param name="fromVertexName"></param>
        /// <param name="fromEndpoints"></param>
        /// <param name="toVertexName"></param>
        /// <param name="toEndpoints"></param>
        /// <returns></returns>
        public CRAErrorCode ConnectShardedVertexesWithFullMesh(string fromVertexName, string[] fromEndpoints, string toVertexName, string[] toEndpoints)
        {
            var fromVertexesRows = VertexTable.GetRowsForShardedVertex(_vertexTable, fromVertexName);
            string[] fromVertexesNames = new string[fromVertexesRows.Count()];

            int i = 0;
            foreach (var fromVertexRow in fromVertexesRows)
            {
                fromVertexesNames[i] = fromVertexRow.VertexName;
                i++;
            }

            var toVertexesRows = VertexTable.GetRowsForShardedVertex(_vertexTable, toVertexName);
            string[] toVertexesNames = new string[toVertexesRows.Count()];

            i = 0;
            foreach (var toVertexRow in toVertexesRows)
            {
                toVertexesNames[i] = toVertexRow.VertexName;
                i++;
            }

            return ConnectShardedVertexesWithFullMesh(fromVertexesNames, fromEndpoints, toVertexesNames, toEndpoints, ConnectionInitiator.FromSide);
        }

        /// <summary>
        /// Connect a set of vertexes in one sharded CRA vertex to another with a full mesh toplogy, via pre-defined endpoints. We contact the "from" vertex
        /// to initiate the creation of the link.
        /// </summary>
        /// <param name="fromVertexesNames"></param>
        /// <param name="fromEndpoints"></param>
        /// <param name="toVertexesNames"></param>
        /// <param name="toEndpoints"></param>
        /// <param name="direction"></param>
        /// <returns></returns>
        public CRAErrorCode ConnectShardedVertexesWithFullMesh(string[] fromVertexesNames, string[] fromEndpoints, string[] toVertexesNames, string[] toEndpoints, ConnectionInitiator direction)
        {
            // Check if the number of output endpoints in any fromVertex is equal to 
            // the number of toVertexes
            if (fromEndpoints.Length != toVertexesNames.Length)
                return CRAErrorCode.VertexesEndpointsNotMatched;

            // Check if the number of input endpoints in any toVertex is equal to 
            // the number of fromVertexes
            if (toEndpoints.Length != fromVertexesNames.Length)
                return CRAErrorCode.VertexesEndpointsNotMatched;

            //Connect the sharded vertexes in parallel
            List<Task<CRAErrorCode>> tasks = new List<Task<CRAErrorCode>>();
            for (int i = 0; i < fromEndpoints.Length; i++)
            {
                for (int j = 0; j < fromVertexesNames.Length; j++)
                {
                    int fromEndpointIndex = i;
                    int fromVertexIndex = j;
                    tasks.Add(ConnectAsync(fromVertexesNames[fromVertexIndex], fromEndpoints[fromEndpointIndex],
                                           toVertexesNames[fromEndpointIndex], toEndpoints[fromVertexIndex], direction));
                }
            }
            CRAErrorCode[] results = Task.WhenAll(tasks.ToArray()).Result;

            //Check for the status of connected vertexes
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
                Console.WriteLine("All CRA instances appear to be down. Restart them and connect these two sharded vertexes again!");

            return result;
        }

        /// <summary>
        /// Disconnect a set of vertexes in one sharded CRA vertex from another, via pre-defined endpoints. 
        /// </summary>
        /// <param name="fromVertexName"></param>
        /// <param name="fromEndpoints"></param>
        /// <param name="toVertexName"></param>
        /// <param name="toEndpoints"></param>
        /// <returns></returns>
        public void DisconnectShardedVertexes(string fromVertexName, string[] fromEndpoints, string toVertexName, string[] toEndpoints)
        {
            var fromVertexesRows = VertexTable.GetRowsForShardedVertex(_vertexTable, fromVertexName);
            string[] fromVertexesNames = new string[fromVertexesRows.Count()];

            int i = 0;
            foreach (var fromVertexRow in fromVertexesRows)
            {
                fromVertexesNames[i] = fromVertexRow.VertexName;
                i++;
            }

            var toVertexesRows = VertexTable.GetRowsForShardedVertex(_vertexTable, toVertexName);
            string[] toVertexesNames = new string[toVertexesRows.Count()];

            i = 0;
            foreach (var toVertexRow in toVertexesRows)
            {
                toVertexesNames[i] = toVertexRow.VertexName;
                i++;
            }

            DisconnectShardedVertexes(fromVertexesNames, fromEndpoints, toVertexesNames, toEndpoints);
        }

        /// <summary>
        /// Disconnect the CRA connections between vertexes in two sharded vertexes
        /// </summary>
        /// <param name="fromVertexesNames"></param>
        /// <param name="fromVertexesOutputs"></param>
        /// <param name="toVertexesNames"></param>
        /// <param name="toVertexesInputs"></param>
        public void DisconnectShardedVertexes(string[] fromVertexesNames, string[] fromVertexesOutputs, string[] toVertexesNames, string[] toVertexesInputs)
        {
            // Check if the number of output endpoints in any fromVertex is equal to 
            // the number of toVertexes, and the number of input endpoints in any toVertex 
            // is equal to the number of fromVertexes
            if ((fromVertexesOutputs.Length == toVertexesNames.Length) && (toVertexesInputs.Length == fromVertexesNames.Length))
            {
                //Disconnect the sharded vertexes in parallel
                List<Task> tasks = new List<Task>();
                for (int i = 0; i < fromVertexesOutputs.Length; i++)
                {
                    for (int j = 0; j < fromVertexesNames.Length; j++)
                    {
                        int fromEndpointIndex = i;
                        int fromVertexIndex = j;
                        tasks.Add(DisconnectAsync(fromVertexesNames[fromVertexIndex], fromVertexesOutputs[fromEndpointIndex],
                                  toVertexesNames[fromEndpointIndex], toVertexesInputs[fromVertexIndex]));
                    }
                }

                Task.WhenAll(tasks.ToArray());
            }
        }


        internal Task DisconnectAsync(string fromVertexName, string fromVertexOutput, string toVertexName, string toVertexInput)
        {
            return Task.Factory.StartNew(
                () => { Disconnect(fromVertexName, fromVertexOutput, toVertexName, toVertexInput); });
        }

    }
}
