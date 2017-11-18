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
                    shuffleTask.SecondaryEndpointsDescriptor.FromInputs = topology.OperatorsEndpointsDescriptors[shuffleTask.ReducerVertexName].FromInputs;
                    topology.OperatorsEndpointsDescriptors[shuffleTask.ReducerVertexName].FromInputs = new ConcurrentDictionary<string, int>();
                    int shardsCount = client.CountVertexShards(shuffleTask.DeployDescriptor.InstancesMap());
                    topology.UpdateShuffleInputs(shuffleTask.MapperVertexName, shuffleTask.ReducerVertexName, shardsCount);
                    topology.UpdateShuffleOperatorTask(shuffleTask.ReducerVertexName, shuffleTask);
                }
            }

            var tasksDictionary = PrepareTasksDictionary(tasks);
            var connectionsMap = PrepareVertexesConnectionsMap(client, tasks, tasksIds, tasksDictionary, topology);

            bool isSuccessful = true;
            for (int i = 0; i < tasks.Length; i++)
            {
                tasks[i].EndpointsDescriptor = topology.OperatorsEndpointsDescriptors[tasksIds[i]];
                tasks[i].VertexesConnectionsMap = connectionsMap;
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
                    dictionary.Add(((ShuffleTask)tasks[i]).ReducerVertexName, tasks[i]);
                else
                    dictionary.Add(tasks[i].OutputId, tasks[i]);
            }

            return dictionary;
        }

        private static ConcurrentDictionary<string, List<ConnectionInfoWithLocality>> PrepareVertexesConnectionsMap(
                        CRAClientLibrary client, TaskBase[] tasks, string[] tasksIds, Dictionary<string, TaskBase> tasksDictionary, OperatorsToplogy topology)
        {
            ConcurrentDictionary<string, List<ConnectionInfoWithLocality>> vertexesConnectionsMap = new ConcurrentDictionary<string, List<ConnectionInfoWithLocality>>();

            for (int i = 0; i < tasks.Length; i++)
            {
                int shardsCount = client.CountVertexShards(tasks[i].DeployDescriptor.InstancesMap());

                tasks[i].EndpointsDescriptor = topology.OperatorsEndpointsDescriptors[tasksIds[i]];
                if (tasks[i].OperationType == OperatorType.Move)
                {
                    var shuffleTask = (ShuffleTask)tasks[i];
                    foreach (string fromInputId in shuffleTask.SecondaryEndpointsDescriptor.FromInputs.Keys)
                    {
                        var flatFromToConnections = PrepareFlatConnectionsMap(client, shardsCount,
                                    fromInputId, topology.OperatorsEndpointsDescriptors[fromInputId], tasksDictionary[fromInputId].DeployDescriptor.InstancesMap(),
                                    shuffleTask.ReducerVertexName, shuffleTask.EndpointsDescriptor, shuffleTask.DeployDescriptor.InstancesMap());
                        vertexesConnectionsMap.AddOrUpdate(fromInputId + shuffleTask.OutputId, 
                                                flatFromToConnections, (key, value) => flatFromToConnections);
                    }

                    var fromInput = shuffleTask.EndpointsDescriptor.FromInputs.First().Key;
                    var newFromInputId = fromInput.Substring(0, fromInput.Length - 1);
                    var shuffleFromToConnections = PrepareShuffleConnectionsMap(client, shardsCount,
                                                    newFromInputId, topology.OperatorsEndpointsDescriptors[newFromInputId], tasksDictionary[newFromInputId].DeployDescriptor.InstancesMap(),
                                                    shuffleTask.ReducerVertexName, shuffleTask.EndpointsDescriptor, shuffleTask.DeployDescriptor.InstancesMap());
                    vertexesConnectionsMap.AddOrUpdate(newFromInputId + shuffleTask.ReducerVertexName, 
                                            shuffleFromToConnections, (key, value) => shuffleFromToConnections);
                }
                else
                {
                    foreach (string fromInputId in tasks[i].EndpointsDescriptor.FromInputs.Keys)
                    {
                        var flatFromToConnections = PrepareFlatConnectionsMap(client, shardsCount,
                                    fromInputId, topology.OperatorsEndpointsDescriptors[fromInputId], tasksDictionary[fromInputId].DeployDescriptor.InstancesMap(),
                                    tasks[i].OutputId, tasks[i].EndpointsDescriptor, tasks[i].DeployDescriptor.InstancesMap());
                        vertexesConnectionsMap.AddOrUpdate(fromInputId + tasks[i].OutputId,
                                                flatFromToConnections, (key, value) => flatFromToConnections);
                    }
                }
            }
            return vertexesConnectionsMap;
        }

        public static bool DeployProduceTask(CRAClientLibrary client, ProduceTask task)
        {
            try { 
                client.DefineVertex(typeof(ProducerOperator).Name.ToLower(), () => new ProducerOperator());
                CRAErrorCode status = client.InstantiateShardedVertex(task.OutputId, typeof(ProducerOperator).Name.ToLower(),
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
                client.DefineVertex(typeof(SubscribeOperator).Name.ToLower(), () => new SubscribeOperator());
                CRAErrorCode status =  client.InstantiateShardedVertex(task.OutputId, typeof(SubscribeOperator).Name.ToLower(),
                                            task, task.DeployDescriptor.InstancesMap());
                if (status == CRAErrorCode.Success)
                {
                    foreach (string fromInputId in task.EndpointsDescriptor.FromInputs.Keys)
                        client.ConnectShardedVertexes(task.VertexesConnectionsMap[fromInputId  + task.OutputId]);

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
                client.DefineVertex(typeof(ShuffleOperator).Name.ToLower(), () => new ShuffleOperator());
                CRAErrorCode status =  client.InstantiateShardedVertex(task.ReducerVertexName, typeof(ShuffleOperator).Name.ToLower(),
                                                task, task.DeployDescriptor.InstancesMap());
                if (status == CRAErrorCode.Success)
                {
                    foreach (string fromInputId in task.SecondaryEndpointsDescriptor.FromInputs.Keys)
                        client.ConnectShardedVertexes(task.VertexesConnectionsMap[fromInputId + task.OutputId]);

                    var fromInput = task.EndpointsDescriptor.FromInputs.First().Key;
                    var newFromInputId = fromInput.Substring(0, fromInput.Length - 1);
                    client.ConnectShardedVertexes(task.VertexesConnectionsMap[newFromInputId + task.ReducerVertexName]);

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
                                                                ref DetachedVertex clientTerminal, OperatorsToplogy topology)
        {
            try
            {
                foreach (string fromInputId in task.EndpointsDescriptor.FromInputs.Keys)
                {
                    string[] inputEndpoints = OperatorUtils.PrepareInputEndpointsIdsForOperator(fromInputId, task.EndpointsDescriptor);
                    string[] outputEndpoints = OperatorUtils.PrepareOutputEndpointsIdsForOperator(
                                    task.OutputId, topology.OperatorsEndpointsDescriptors[fromInputId]);
                    int shardsCount = client.CountVertexShards(task.DeployDescriptor.InstancesMap());
                    for (int i = 0; i < shardsCount; i++)
                        for (int j = 0; j < inputEndpoints.Length; j++)
                            clientTerminal.FromRemoteOutputEndpointStream(inputEndpoints[j] + i, fromInputId + "$" + i, outputEndpoints[j]);
                }
                return true;
            }
            catch (Exception e)
            {
                Console.WriteLine("Error in deploying a CRA client terminal vertex. Please, double check your task configurations: " + e.ToString());
                return false;
            }
        }

        private static List<ConnectionInfoWithLocality> PrepareFlatConnectionsMap(CRAClientLibrary client, int shardsCount, 
                            string fromVertex, OperatorEndpointsDescriptor fromVertexDescriptor, ConcurrentDictionary<string, int> fromVertexShards,
                            string toVertex, OperatorEndpointsDescriptor toVertexDescriptor, ConcurrentDictionary<string, int> toVertexShards)
        {
            List<ConnectionInfoWithLocality> fromToConnections = new List<ConnectionInfoWithLocality>();
            string[] outputs = OperatorUtils.PrepareOutputEndpointsIdsForOperator(
                                                            toVertex, fromVertexDescriptor);
            string[] inputs = OperatorUtils.PrepareInputEndpointsIdsForOperator(
                                                            fromVertex, toVertexDescriptor);
            for (int i = 0; i < shardsCount; i++)
            {
                string currentFromVertex = fromVertex + "$" + i;
                string currentToVertex = toVertex + "$" + i;
                bool hasSameCRAInstances = client.AreTwoVertexessOnSameCRAInstance(currentFromVertex, fromVertexShards, currentToVertex, toVertexShards);
                for (int j = 0; j < outputs.Length; j++)
                {
                    var fromVertexTuple = new Tuple<string, string>(currentFromVertex, outputs[j]);
                    var toVertexTuple = new Tuple<string, string>(currentToVertex, inputs[j]);
                    fromToConnections.Add(new ConnectionInfoWithLocality(currentFromVertex, outputs[j], currentToVertex, inputs[j], hasSameCRAInstances));
                }
            }
            return fromToConnections;
        }

        private static List<ConnectionInfoWithLocality> PrepareShuffleConnectionsMap(CRAClientLibrary client, int shardsCount, 
                            string fromVertex, OperatorEndpointsDescriptor fromVertexDescriptor, ConcurrentDictionary<string, int> fromVertexShards,
                            string toVertex, OperatorEndpointsDescriptor toVertexDescriptor, ConcurrentDictionary<string, int> toVertexShards)
        {
           List<ConnectionInfoWithLocality> fromToConnections = new List<ConnectionInfoWithLocality>();
            for (int i = 0; i < shardsCount; i++)
            {
                for (int j = 0; j < shardsCount; j++)
                {
                    string currentFromVertex = fromVertex + "$" + i;
                    string currentToVertex = toVertex + "$" + j;
                    string[] currentOutputs = OperatorUtils.PrepareOutputEndpointsIdsForOperator(
                                                                toVertex + j, fromVertexDescriptor);
                    string[] currentInputs = OperatorUtils.PrepareInputEndpointsIdsForOperator(
                                                                fromVertex + i, toVertexDescriptor);
                    bool hasSameCRAInstances = client.AreTwoVertexessOnSameCRAInstance(currentFromVertex, fromVertexShards, currentToVertex, toVertexShards);
                    for (int k = 0; k < currentOutputs.Length; k++)
                        fromToConnections.Add(new ConnectionInfoWithLocality(currentFromVertex, currentOutputs[k], currentToVertex, currentInputs[k], hasSameCRAInstances));
                }
            }

            return fromToConnections;
        }
    }
}
