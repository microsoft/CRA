using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;

namespace CRA.ClientLibrary.DataProcessing
{
    public interface IDeployDescriptor
    {
        ConcurrentDictionary<string, int> InstancesMap();
    }

    [DataContract]
    public class DeployDescriptorBase : IDeployDescriptor
    {
        [DataMember]
        ConcurrentDictionary<string, int> _deployDescriptor;

        public DeployDescriptorBase(ConcurrentDictionary<string, int> deployDescriptor)
        {
            _deployDescriptor = deployDescriptor;
        }

        public ConcurrentDictionary<string, int> InstancesMap()
        {
            return _deployDescriptor;
        }
    }

    public static class DeploymentUtils
    {
        public static IDeployDescriptor DefaultDeployDescriptor { get; set; }

        public static IDeployDescriptor CreateDefaultDeployDescriptor()
        {
            if (DefaultDeployDescriptor == null)
            {
                ConcurrentDictionary<string, int> deployShards = new ConcurrentDictionary<string, int>();
                deployShards.AddOrUpdate("crainst01", 1, (inst, proc) => 1);
                return new DeployDescriptorBase(deployShards);
            }
            else
                return DefaultDeployDescriptor;
        }

        public static bool DeployOperators(CRAClientLibrary client, OperatorsToplogy topology)
        {
            topology.PrepareFinalOperatorsTasks();

            string[] tasksIds = topology.OperatorsIds.ToArray();
            TaskBase[] tasks = topology.OperatorsTasks.ToArray();

            for (int i = 0; i < tasks.Length; i++)
            {
                if (tasks[i].OperationType == OperatorType.Move)
                {
                    ShuffleTask shuffleTask = (ShuffleTask)tasks[i];
                    shuffleTask.SecondaryEndpointsDescriptor = new OperatorEndpointsDescriptor();
                    shuffleTask.SecondaryEndpointsDescriptor.FromInputs = topology.OperatorsEndpointsDescriptors[shuffleTask.ReducerProcessName].FromInputs;
                    topology.OperatorsEndpointsDescriptors[shuffleTask.ReducerProcessName].FromInputs = new ConcurrentDictionary<string, int>();
                    int shardsCount = client.CountProcessShards(shuffleTask.DeployDescriptor.InstancesMap());
                    topology.UpdateShuffleInputs(shuffleTask.MapperProcessName, shuffleTask.ReducerProcessName, shardsCount);
                    topology.UpdateShuffleOperatorTask(shuffleTask.ReducerProcessName, shuffleTask);
                }
            }

            var tasksDictionary = PrepareTasksDictionary(tasks);
            var connectionsMap = PrepareProcessesConnectionsMap(client, tasks, tasksIds, tasksDictionary, topology);

            bool isSuccessful = true;
            for (int i = 0; i < tasks.Length; i++)
            {
                tasks[i].EndpointsDescriptor = topology.OperatorsEndpointsDescriptors[tasksIds[i]];
                tasks[i].ProcessesConnectionsMap = connectionsMap;
                if (tasks[i].OperationType == OperatorType.Produce)
                    isSuccessful = DeployProduceTask(client, (ProduceTask)tasks[i]);
                else if (tasks[i].OperationType == OperatorType.Subscribe)
                    isSuccessful = DeploySubscribeTask(client, (SubscribeTask)tasks[i], topology);
                else if (tasks[i].OperationType == OperatorType.Move)
                    isSuccessful = DeployShuffleReduceTask(client, (ShuffleTask)tasks[i], topology);

                if (!isSuccessful) break;
            }

            return isSuccessful;
        }

        private static Dictionary<string, TaskBase> PrepareTasksDictionary(TaskBase[] tasks)
        {
            Dictionary<string, TaskBase> dictionary = new Dictionary<string, TaskBase>();
            for (int i = 0; i < tasks.Length; i++)
            {
                if (tasks[i].OperationType == OperatorType.Move)
                    dictionary.Add(((ShuffleTask)tasks[i]).ReducerProcessName, tasks[i]);
                else
                    dictionary.Add(tasks[i].OutputId, tasks[i]);
            }

            return dictionary;
        }

        private static ConcurrentDictionary<string, List<ConnectionInfoWithLocality>> PrepareProcessesConnectionsMap(
                        CRAClientLibrary client, TaskBase[] tasks, string[] tasksIds, Dictionary<string, TaskBase> tasksDictionary, OperatorsToplogy topology)
        {
            ConcurrentDictionary<string, List<ConnectionInfoWithLocality>> processesConnectionsMap = new ConcurrentDictionary<string, List<ConnectionInfoWithLocality>>();

            for (int i = 0; i < tasks.Length; i++)
            {
                int shardsCount = client.CountProcessShards(tasks[i].DeployDescriptor.InstancesMap());

                tasks[i].EndpointsDescriptor = topology.OperatorsEndpointsDescriptors[tasksIds[i]];
                if (tasks[i].OperationType == OperatorType.Move)
                {
                    var shuffleTask = (ShuffleTask)tasks[i];
                    foreach (string fromInputId in shuffleTask.SecondaryEndpointsDescriptor.FromInputs.Keys)
                    {
                        var flatFromToConnections = PrepareFlatConnectionsMap(client, shardsCount,
                                    fromInputId, topology.OperatorsEndpointsDescriptors[fromInputId], tasksDictionary[fromInputId].DeployDescriptor.InstancesMap(),
                                    shuffleTask.ReducerProcessName, shuffleTask.EndpointsDescriptor, shuffleTask.DeployDescriptor.InstancesMap());
                        processesConnectionsMap.AddOrUpdate(fromInputId + shuffleTask.OutputId, 
                                                flatFromToConnections, (key, value) => flatFromToConnections);
                    }

                    var fromInput = shuffleTask.EndpointsDescriptor.FromInputs.First().Key;
                    var newFromInputId = fromInput.Substring(0, fromInput.Length - 1);
                    var shuffleFromToConnections = PrepareShuffleConnectionsMap(client, shardsCount,
                                                    newFromInputId, topology.OperatorsEndpointsDescriptors[newFromInputId], tasksDictionary[newFromInputId].DeployDescriptor.InstancesMap(),
                                                    shuffleTask.ReducerProcessName, shuffleTask.EndpointsDescriptor, shuffleTask.DeployDescriptor.InstancesMap());
                    processesConnectionsMap.AddOrUpdate(newFromInputId + shuffleTask.ReducerProcessName, 
                                            shuffleFromToConnections, (key, value) => shuffleFromToConnections);
                }
                else
                {
                    foreach (string fromInputId in tasks[i].EndpointsDescriptor.FromInputs.Keys)
                    {
                        var flatFromToConnections = PrepareFlatConnectionsMap(client, shardsCount,
                                    fromInputId, topology.OperatorsEndpointsDescriptors[fromInputId], tasksDictionary[fromInputId].DeployDescriptor.InstancesMap(),
                                    tasks[i].OutputId, tasks[i].EndpointsDescriptor, tasks[i].DeployDescriptor.InstancesMap());
                        processesConnectionsMap.AddOrUpdate(fromInputId + tasks[i].OutputId,
                                                flatFromToConnections, (key, value) => flatFromToConnections);
                    }
                }
            }
            return processesConnectionsMap;
        }

        public static bool DeployProduceTask(CRAClientLibrary client, ProduceTask task)
        {
            try { 
                client.DefineProcess(typeof(ProducerOperator).Name.ToLower(), () => new ProducerOperator());
                CRAErrorCode status = client.InstantiateShardedProcess(task.OutputId, typeof(ProducerOperator).Name.ToLower(),
                                                                task, task.DeployDescriptor.InstancesMap());
                if (status == CRAErrorCode.Success)
                    return true;
                else
                    return false;
            }
            catch(Exception e)
            {
                Console.WriteLine("Error in deploying a sharded CRA produce task. Please, double check your task configurations: " + e.ToString());
                return false;
            }
        }

        public static bool DeploySubscribeTask(CRAClientLibrary client, SubscribeTask task, OperatorsToplogy topology)
        {
            try
            {
                client.DefineProcess(typeof(SubscribeOperator).Name.ToLower(), () => new SubscribeOperator());
                CRAErrorCode status =  client.InstantiateShardedProcess(task.OutputId, typeof(SubscribeOperator).Name.ToLower(),
                                            task, task.DeployDescriptor.InstancesMap());
                if (status == CRAErrorCode.Success)
                {
                    foreach (string fromInputId in task.EndpointsDescriptor.FromInputs.Keys)
                        client.ConnectShardedProcesses(task.ProcessesConnectionsMap[fromInputId  + task.OutputId]);

                    return true;
                }
                else
                    return false;
            }
            catch (Exception e)
            {
                Console.WriteLine("Error in deploying a sharded CRA Subscribe task. Please, double check your task configurations: " + e.ToString());
                return false;
            }
        }

        private static bool DeployShuffleReduceTask(CRAClientLibrary client, ShuffleTask task, OperatorsToplogy topology)
        {
            try
            {
                client.DefineProcess(typeof(ShuffleOperator).Name.ToLower(), () => new ShuffleOperator());
                CRAErrorCode status =  client.InstantiateShardedProcess(task.ReducerProcessName, typeof(ShuffleOperator).Name.ToLower(),
                                                task, task.DeployDescriptor.InstancesMap());
                if (status == CRAErrorCode.Success)
                {
                    foreach (string fromInputId in task.SecondaryEndpointsDescriptor.FromInputs.Keys)
                        client.ConnectShardedProcesses(task.ProcessesConnectionsMap[fromInputId + task.OutputId]);

                    var fromInput = task.EndpointsDescriptor.FromInputs.First().Key;
                    var newFromInputId = fromInput.Substring(0, fromInput.Length - 1);
                    client.ConnectShardedProcesses(task.ProcessesConnectionsMap[newFromInputId + task.ReducerProcessName]);

                    return true;
                }
                else
                    return false;
            }
            catch(Exception)
            {
                Console.WriteLine("Error in deploying a sharded CRA shuffle mapper task. Please, double check your task configurations");
                return false;
            }
        }

        public static bool DeployClientTerminal(CRAClientLibrary client, ClientTerminalTask task,
                                                                ref DetachedProcess clientTerminal, OperatorsToplogy topology)
        {
            try
            {
                foreach (string fromInputId in task.EndpointsDescriptor.FromInputs.Keys)
                {
                    string[] inputEndpoints = OperatorUtils.PrepareInputEndpointsIdsForOperator(fromInputId, task.EndpointsDescriptor);
                    string[] outputEndpoints = OperatorUtils.PrepareOutputEndpointsIdsForOperator(
                                    task.OutputId, topology.OperatorsEndpointsDescriptors[fromInputId]);
                    int shardsCount = client.CountProcessShards(task.DeployDescriptor.InstancesMap());
                    for (int i = 0; i < shardsCount; i++)
                        for (int j = 0; j < inputEndpoints.Length; j++)
                            clientTerminal.FromRemoteOutputEndpointStream(inputEndpoints[j] + i, fromInputId + "$" + i, outputEndpoints[j]);
                }
                return true;
            }
            catch (Exception e)
            {
                Console.WriteLine("Error in deploying a CRA client terminal process. Please, double check your task configurations: " + e.ToString());
                return false;
            }
        }

        private static List<ConnectionInfoWithLocality> PrepareFlatConnectionsMap(CRAClientLibrary client, int shardsCount, 
                            string fromProcess, OperatorEndpointsDescriptor fromProcessDescriptor, ConcurrentDictionary<string, int> fromProcessShards,
                            string toProcess, OperatorEndpointsDescriptor toProcessDescriptor, ConcurrentDictionary<string, int> toProcessShards)
        {
            List<ConnectionInfoWithLocality> fromToConnections = new List<ConnectionInfoWithLocality>();
            string[] outputs = OperatorUtils.PrepareOutputEndpointsIdsForOperator(
                                                            toProcess, fromProcessDescriptor);
            string[] inputs = OperatorUtils.PrepareInputEndpointsIdsForOperator(
                                                            fromProcess, toProcessDescriptor);
            for (int i = 0; i < shardsCount; i++)
            {
                string currentFromProcess = fromProcess + "$" + i;
                string currentToProcess = toProcess + "$" + i;
                bool hasSameCRAInstances = client.AreTwoProcessessOnSameCRAInstance(currentFromProcess, fromProcessShards, currentToProcess, toProcessShards);
                for (int j = 0; j < outputs.Length; j++)
                {
                    var fromProcessTuple = new Tuple<string, string>(currentFromProcess, outputs[j]);
                    var toProcessTuple = new Tuple<string, string>(currentToProcess, inputs[j]);
                    fromToConnections.Add(new ConnectionInfoWithLocality(currentFromProcess, outputs[j], currentToProcess, inputs[j], hasSameCRAInstances));
                }
            }
            return fromToConnections;
        }

        private static List<ConnectionInfoWithLocality> PrepareShuffleConnectionsMap(CRAClientLibrary client, int shardsCount, 
                            string fromProcess, OperatorEndpointsDescriptor fromProcessDescriptor, ConcurrentDictionary<string, int> fromProcessShards,
                            string toProcess, OperatorEndpointsDescriptor toProcessDescriptor, ConcurrentDictionary<string, int> toProcessShards)
        {
           List<ConnectionInfoWithLocality> fromToConnections = new List<ConnectionInfoWithLocality>();
            for (int i = 0; i < shardsCount; i++)
            {
                for (int j = 0; j < shardsCount; j++)
                {
                    string currentFromProcess = fromProcess + "$" + i;
                    string currentToProcess = toProcess + "$" + j;
                    string[] currentOutputs = OperatorUtils.PrepareOutputEndpointsIdsForOperator(
                                                                toProcess + j, fromProcessDescriptor);
                    string[] currentInputs = OperatorUtils.PrepareInputEndpointsIdsForOperator(
                                                                fromProcess + i, toProcessDescriptor);
                    bool hasSameCRAInstances = client.AreTwoProcessessOnSameCRAInstance(currentFromProcess, fromProcessShards, currentToProcess, toProcessShards);
                    for (int k = 0; k < currentOutputs.Length; k++)
                        fromToConnections.Add(new ConnectionInfoWithLocality(currentFromProcess, currentOutputs[k], currentToProcess, currentInputs[k], hasSameCRAInstances));
                }
            }

            return fromToConnections;
        }
    }
}
