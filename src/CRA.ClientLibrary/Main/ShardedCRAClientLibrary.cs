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
        /// Instantiate a sharded process on a set of CRA instances.
        /// </summary>
        /// <param name="processName">Name of the sharded process (particular instance)</param>
        /// <param name="processDefinition">Definition of the process (type)</param>
        /// <param name="processParameter">Parameters to be passed to each process of the sharded process in its constructor (serializable object)</param>
        /// <param name="processShards"> A map that holds the number of processes from a sharded process needs to be instantiated for each CRA instance </param>
        /// <returns>Status of the command</returns>
        public CRAErrorCode InstantiateShardedProcess<ProcessParameterType>(string processName, string processDefinition, ProcessParameterType processParameter, ConcurrentDictionary<string, int> processShards)
        {
            int processesCount = CountProcessShards(processShards);
            Task<CRAErrorCode>[] tasks = new Task<CRAErrorCode>[processesCount];
            int currentCount = 0;
            foreach (var key in processShards.Keys)
            {
                for (int i = currentCount; i < currentCount + processShards[key]; i++)
                {
                    int threadIndex = i; string currInstanceName = key;
                    tasks[threadIndex] = InstantiateProcessAsync(currInstanceName, processName + "$" + threadIndex,
                                                                        processDefinition, new Tuple<int, ProcessParameterType>(threadIndex, processParameter));
                }
                currentCount += processShards[key];
            }
            CRAErrorCode[] results = Task.WhenAll(tasks).Result;

            // Check for the status of instantiated processes
            CRAErrorCode result = CRAErrorCode.ConnectionEstablishFailed;
            for (int i = 0; i < results.Length; i++)
            {
                if (results[i] != CRAErrorCode.Success)
                {
                    Console.WriteLine("We received an error code " + results[i] + " from one CRA instance while instantiating the process " + processName + "$" + i + " in the sharded process");
                    result = results[i];
                }
                else
                    result = CRAErrorCode.Success;
            }

            if (result != CRAErrorCode.Success)
                Console.WriteLine("All CRA instances appear to be down. Restart them and this sharded process will be automatically instantiated");

            return result;
        }

        internal Task<CRAErrorCode> InstantiateProcessAsync(string instanceName, string processName, string processDefinition, object processParameter)
        {
            return Task.Factory.StartNew(
                () => { return InstantiateProcess(instanceName, processName, processDefinition, processParameter); });
        }

        internal int CountProcessShards(ConcurrentDictionary<string, int> processesPerInstanceMap)
        {
            int count = 0;
            foreach (var key in processesPerInstanceMap.Keys)
            {
                count += processesPerInstanceMap[key];
            }
            return count;
        }
        public bool AreTwoProcessessOnSameCRAInstance(string fromProcessName, ConcurrentDictionary<string, int> fromProcessShards, string toProcessName, ConcurrentDictionary<string, int> toProcessShards)
        {
            string fromProcessInstance = null;
            string fromProcessId = fromProcessName.Substring(fromProcessName.Length - 2);
            int fromProcessesCount = CountProcessShards(fromProcessShards);
            int currentCount = 0;
            foreach (var key in fromProcessShards.Keys)
            {
                for (int i = currentCount; i < currentCount + fromProcessShards[key]; i++)
                {
                    if (fromProcessId.Equals("$" + i))
                    {
                        fromProcessInstance = key;
                        break;
                    }
                }

                if (fromProcessInstance != null)
                    break;

                currentCount += fromProcessShards[key];
            }

            string toProcessInstance = null;
            string toProcessId = toProcessName.Substring(toProcessName.Length - 2);
            int toProcessesCount = CountProcessShards(toProcessShards);
            currentCount = 0;
            foreach (var key in toProcessShards.Keys)
            {
                for (int i = currentCount; i < currentCount + toProcessShards[key]; i++)
                {
                    if (toProcessId.Equals("$" + i))
                    {
                        toProcessInstance = key;
                        break;
                    }
                }

                if (toProcessInstance != null)
                    break;

                currentCount += toProcessShards[key];
            }

            return (fromProcessInstance != null) && (toProcessInstance != null) && (fromProcessInstance.Equals(toProcessInstance));
        }

        /// <summary>
        /// Delete all processes in a sharded process from a set of CRA instances
        /// </summary>
        /// <param name="processName"></param>
        /// <param name="instancesNames"></param>
        public void DeleteShardedProcessFromInstances(string processName, string[] instancesNames)
        {
            Task[] tasks = new Task[instancesNames.Length];
            for (int i = 0; i < tasks.Length; i++)
            {
                int threadIndex = i;
                tasks[threadIndex] = DeleteProcessesFromInstanceAsync(processName, instancesNames[threadIndex]);
            }
            Task.WhenAll(tasks);
        }

        internal Task DeleteProcessesFromInstanceAsync(string processName, string instanceName)
        {
            return Task.Factory.StartNew(
                () => { DeleteProcessesFromInstance(processName, instanceName); });
        }

        private void DeleteProcessesFromInstance(string processName, string instanceName)
        {
            Expression<Func<DynamicTableEntity, bool>> processesFilter = (e => e.PartitionKey == instanceName && e.RowKey.StartsWith(processName + "$"));
            FilterAndDeleteEntitiesInBatches(_processTable, processesFilter);
        }

        private void FilterAndDeleteEntitiesInBatches(CloudTable table, Expression<Func<DynamicTableEntity, bool>> filter)
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

            FilterAndProcessEntitiesInSegments(table, deleteProcessor, filter);
        }


        private void FilterAndProcessEntitiesInSegments(CloudTable table, Action<IEnumerable<DynamicTableEntity>> operationExecutor, Expression<Func<DynamicTableEntity, bool>> filter)
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
        /// Delete an endpoint from all processes in a sharded process
        /// </summary>
        /// <param name="processName"></param>
        /// <param name="endpointName"></param>
        public void DeleteEndpointFromShardedProcess(string processName, string endpointName)
        {
            DeleteEndpointsFromAllProcesses(processName, endpointName);
        }

        private void DeleteEndpointsFromAllProcesses(string processName, string endpointName)
        {
            Expression<Func<DynamicTableEntity, bool>> filters = (e => e.PartitionKey.StartsWith(processName + "$") && e.RowKey == endpointName);
            FilterAndDeleteEntitiesInBatches(_endpointTableManager.EndpointTable, filters);
        }

        /// <summary>
        /// Connect a set of (fromProcess, outputEndpoint) pairs to another set of (toProcess, inputEndpoint) pairs. We contact the "from" process
        /// to initiate the creation of the link.
        /// </summary>
        /// <param name="fromToConnections"></param>
        /// <returns></returns>
        public CRAErrorCode ConnectShardedProcesses(List<ConnectionInfoWithLocality> fromToConnections)
        {
            Task<CRAErrorCode>[] tasks = new Task<CRAErrorCode>[fromToConnections.Count];

            int i = 0;
            foreach (var fromToConnection in fromToConnections)
            {
                int fromToEndpointIndex = i;
                var currentFromToConnection = fromToConnection;
                tasks[fromToEndpointIndex] = ConnectAsync(currentFromToConnection.FromProcess,
                                                currentFromToConnection.FromEndpoint, currentFromToConnection.ToProcess,
                                                currentFromToConnection.ToEndpoint, ConnectionInitiator.FromSide);
                i++;
            }
            CRAErrorCode[] results = Task.WhenAll(tasks).Result;

            CRAErrorCode result = CRAErrorCode.ProcessEndpointNotFound;
            for (i = 0; i < results.Length; i++)
            {
                if (results[i] != CRAErrorCode.Success)
                {
                    Console.WriteLine("We received an error code " + results[i] + " while processing the connection pair with number (" + i + ")");
                    result = results[i];
                }
                else
                    result = CRAErrorCode.Success;
            }

            if (result != CRAErrorCode.Success)
                Console.WriteLine("All CRA instances appear to be down. Restart them and connect these two sharded processes again!");

            return result;
        }

        internal Task<CRAErrorCode> ConnectAsync(string fromProcessName, string fromEndpoint, string toProcessName, string toEndpoint, ConnectionInitiator direction)
        {
            return Task.Factory.StartNew(
                () => { return Connect(fromProcessName, fromEndpoint, toProcessName, toEndpoint, direction); });
        }


        /// <summary>
        /// Connect a set of processes in one sharded CRA process to another with a full mesh topology, via pre-defined endpoints. We contact the "from" process
        /// to initiate the creation of the link.
        /// </summary>
        /// <param name="fromProcessName"></param>
        /// <param name="fromEndpoints"></param>
        /// <param name="toProcessName"></param>
        /// <param name="toEndpoints"></param>
        /// <returns></returns>
        public CRAErrorCode ConnectShardedProcessesWithFullMesh(string fromProcessName, string[] fromEndpoints, string toProcessName, string[] toEndpoints)
        {
            var fromProcessesRows = ProcessTable.GetRowsForShardedProcess(_processTable, fromProcessName);
            string[] fromProcessesNames = new string[fromProcessesRows.Count()];

            int i = 0;
            foreach (var fromProcessRow in fromProcessesRows)
            {
                fromProcessesNames[i] = fromProcessRow.ProcessName;
                i++;
            }

            var toProcessesRows = ProcessTable.GetRowsForShardedProcess(_processTable, toProcessName);
            string[] toProcessesNames = new string[toProcessesRows.Count()];

            i = 0;
            foreach (var toProcessRow in toProcessesRows)
            {
                toProcessesNames[i] = toProcessRow.ProcessName;
                i++;
            }

            return ConnectShardedProcessesWithFullMesh(fromProcessesNames, fromEndpoints, toProcessesNames, toEndpoints, ConnectionInitiator.FromSide);
        }

        /// <summary>
        /// Connect a set of processes in one sharded CRA process to another with a full mesh toplogy, via pre-defined endpoints. We contact the "from" process
        /// to initiate the creation of the link.
        /// </summary>
        /// <param name="fromProcessesNames"></param>
        /// <param name="fromEndpoints"></param>
        /// <param name="toProcessesNames"></param>
        /// <param name="toEndpoints"></param>
        /// <param name="direction"></param>
        /// <returns></returns>
        public CRAErrorCode ConnectShardedProcessesWithFullMesh(string[] fromProcessesNames, string[] fromEndpoints, string[] toProcessesNames, string[] toEndpoints, ConnectionInitiator direction)
        {
            // Check if the number of output endpoints in any fromProcess is equal to 
            // the number of toProcesses
            if (fromEndpoints.Length != toProcessesNames.Length)
                return CRAErrorCode.ProcessesEndpointsNotMatched;

            // Check if the number of input endpoints in any toProcess is equal to 
            // the number of fromProcesses
            if (toEndpoints.Length != fromProcessesNames.Length)
                return CRAErrorCode.ProcessesEndpointsNotMatched;

            //Connect the sharded processes in parallel
            List<Task<CRAErrorCode>> tasks = new List<Task<CRAErrorCode>>();
            for (int i = 0; i < fromEndpoints.Length; i++)
            {
                for (int j = 0; j < fromProcessesNames.Length; j++)
                {
                    int fromEndpointIndex = i;
                    int fromProcessIndex = j;
                    tasks.Add(ConnectAsync(fromProcessesNames[fromProcessIndex], fromEndpoints[fromEndpointIndex],
                                           toProcessesNames[fromEndpointIndex], toEndpoints[fromProcessIndex], direction));
                }
            }
            CRAErrorCode[] results = Task.WhenAll(tasks.ToArray()).Result;

            //Check for the status of connected processes
            CRAErrorCode result = CRAErrorCode.ProcessEndpointNotFound;
            for (int i = 0; i < results.Length; i++)
            {
                if (results[i] != CRAErrorCode.Success)
                {
                    Console.WriteLine("We received an error code " + results[i] + " while processing the connection pair with number (" + i + ")");
                    result = results[i];
                }
                else
                    result = CRAErrorCode.Success;
            }

            if (result != CRAErrorCode.Success)
                Console.WriteLine("All CRA instances appear to be down. Restart them and connect these two sharded processes again!");

            return result;
        }

        /// <summary>
        /// Disconnect a set of processes in one sharded CRA process from another, via pre-defined endpoints. 
        /// </summary>
        /// <param name="fromProcessName"></param>
        /// <param name="fromEndpoints"></param>
        /// <param name="toProcessName"></param>
        /// <param name="toEndpoints"></param>
        /// <returns></returns>
        public void DisconnectShardedProcesses(string fromProcessName, string[] fromEndpoints, string toProcessName, string[] toEndpoints)
        {
            var fromProcessesRows = ProcessTable.GetRowsForShardedProcess(_processTable, fromProcessName);
            string[] fromProcessesNames = new string[fromProcessesRows.Count()];

            int i = 0;
            foreach (var fromProcessRow in fromProcessesRows)
            {
                fromProcessesNames[i] = fromProcessRow.ProcessName;
                i++;
            }

            var toProcessesRows = ProcessTable.GetRowsForShardedProcess(_processTable, toProcessName);
            string[] toProcessesNames = new string[toProcessesRows.Count()];

            i = 0;
            foreach (var toProcessRow in toProcessesRows)
            {
                toProcessesNames[i] = toProcessRow.ProcessName;
                i++;
            }

            DisconnectShardedProcesses(fromProcessesNames, fromEndpoints, toProcessesNames, toEndpoints);
        }

        /// <summary>
        /// Disconnect the CRA connections between processes in two sharded processes
        /// </summary>
        /// <param name="fromProcessesNames"></param>
        /// <param name="fromProcessesOutputs"></param>
        /// <param name="toProcessesNames"></param>
        /// <param name="toProcessesInputs"></param>
        public void DisconnectShardedProcesses(string[] fromProcessesNames, string[] fromProcessesOutputs, string[] toProcessesNames, string[] toProcessesInputs)
        {
            // Check if the number of output endpoints in any fromProcess is equal to 
            // the number of toProcesses, and the number of input endpoints in any toProcess 
            // is equal to the number of fromProcesses
            if ((fromProcessesOutputs.Length == toProcessesNames.Length) && (toProcessesInputs.Length == fromProcessesNames.Length))
            {
                //Disconnect the sharded processes in parallel
                List<Task> tasks = new List<Task>();
                for (int i = 0; i < fromProcessesOutputs.Length; i++)
                {
                    for (int j = 0; j < fromProcessesNames.Length; j++)
                    {
                        int fromEndpointIndex = i;
                        int fromProcessIndex = j;
                        tasks.Add(DisconnectAsync(fromProcessesNames[fromProcessIndex], fromProcessesOutputs[fromEndpointIndex],
                                  toProcessesNames[fromEndpointIndex], toProcessesInputs[fromProcessIndex]));
                    }
                }

                Task.WhenAll(tasks.ToArray());
            }
        }


        internal Task DisconnectAsync(string fromProcessName, string fromProcessOutput, string toProcessName, string toProcessInput)
        {
            return Task.Factory.StartNew(
                () => { Disconnect(fromProcessName, fromProcessOutput, toProcessName, toProcessInput); });
        }

    }
}
