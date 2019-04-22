using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading.Tasks;

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
                deployShards.AddOrUpdate("crainst0", 1, (inst, proc) => 1);
                return new DeployDescriptorBase(deployShards);
            }
            else
                return DefaultDeployDescriptor;
        }

        public static async Task<bool> DeployOperators(CRAClientLibrary client, OperatorsToplogy topology)
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
            var connectionsMap = PrepareVerticesConnectionsMap(client, tasks, tasksIds, tasksDictionary, topology);

            bool isSuccessful = true;
            for (int i = 0; i < tasks.Length; i++)
            {
                tasks[i].EndpointsDescriptor = topology.OperatorsEndpointsDescriptors[tasksIds[i]];
                tasks[i].VerticesConnectionsMap = connectionsMap;
                if (tasks[i].OperationType == OperatorType.Produce)
                    isSuccessful = await DeployProduceTask(client, (ProduceTask)tasks[i]);
                else if (tasks[i].OperationType == OperatorType.Subscribe)
                    isSuccessful = await DeploySubscribeTask(client, (SubscribeTask)tasks[i], topology);
                else if (tasks[i].OperationType == OperatorType.Move)
                    isSuccessful = await DeployShuffleReduceTask(client, (ShuffleTask)tasks[i], topology);

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

        private static ConcurrentDictionary<string, List<ConnectionInfoWithLocality>> PrepareVerticesConnectionsMap(
                        CRAClientLibrary client, TaskBase[] tasks, string[] tasksIds, Dictionary<string, TaskBase> tasksDictionary, OperatorsToplogy topology)
        {
            ConcurrentDictionary<string, List<ConnectionInfoWithLocality>> verticesConnectionsMap = new ConcurrentDictionary<string, List<ConnectionInfoWithLocality>>();

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
                        verticesConnectionsMap.AddOrUpdate(fromInputId + shuffleTask.OutputId, 
                                                flatFromToConnections, (key, value) => flatFromToConnections);
                    }

                    var fromInput = shuffleTask.EndpointsDescriptor.FromInputs.First().Key;
                    var newFromInputId = fromInput.Substring(0, fromInput.Length - 1);
                    var shuffleFromToConnections = PrepareShuffleConnectionsMap(client, shardsCount,
                                                    newFromInputId, topology.OperatorsEndpointsDescriptors[newFromInputId], tasksDictionary[newFromInputId].DeployDescriptor.InstancesMap(),
                                                    shuffleTask.ReducerVertexName, shuffleTask.EndpointsDescriptor, shuffleTask.DeployDescriptor.InstancesMap());
                    verticesConnectionsMap.AddOrUpdate(newFromInputId + shuffleTask.ReducerVertexName, 
                                            shuffleFromToConnections, (key, value) => shuffleFromToConnections);
                }
                else
                {
                    foreach (string fromInputId in tasks[i].EndpointsDescriptor.FromInputs.Keys)
                    {
                        var flatFromToConnections = PrepareFlatConnectionsMap(client, shardsCount,
                                    fromInputId, topology.OperatorsEndpointsDescriptors[fromInputId], tasksDictionary[fromInputId].DeployDescriptor.InstancesMap(),
                                    tasks[i].OutputId, tasks[i].EndpointsDescriptor, tasks[i].DeployDescriptor.InstancesMap());
                        verticesConnectionsMap.AddOrUpdate(fromInputId + tasks[i].OutputId,
                                                flatFromToConnections, (key, value) => flatFromToConnections);
                    }
                }
            }
            return verticesConnectionsMap;
        }

        public static async Task<bool> DeployProduceTask(CRAClientLibrary client, ProduceTask task) {
            try
            {
                await client.DefineVertex(typeof(ShardedProducerOperator).Name.ToLower(), () => new ShardedProducerOperator());
                var status = await client.InstantiateVertex(CreateInstancesNames(task.DeployDescriptor.InstancesMap()), task.OutputId, typeof(ShardedProducerOperator).Name.ToLower(), task, 1);
                if (status == CRAErrorCode.Success)
                    return true;
                else
                    return false;
            }
            catch (Exception e)
            {
                Console.WriteLine("Error in deploying a sharded CRA produce task. Please, double check your task configurations: " + e.ToString());
                return false;
            }
        }

        public static async Task<bool> DeployProduceTaskOld(CRAClientLibrary client, ProduceTask task)
        {
            try { 
                await client
                    .DefineVertex(
                        typeof(ProducerOperator).Name.ToLower(),
                        () => new ProducerOperator(client.DataProvider));

                CRAErrorCode status = client.InstantiateShardedVertex(
                    task.OutputId,
                    typeof(ProducerOperator).Name.ToLower(),
                    task,
                    task.DeployDescriptor.InstancesMap());

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

        public static async Task<bool> DeploySubscribeTask(CRAClientLibrary client, SubscribeTask task, OperatorsToplogy topology)
        {
            try
            {
                await client.DefineVertex(typeof(ShardedSubscribeOperator).Name.ToLower(), () => new ShardedSubscribeOperator());
                var status = await client.InstantiateVertex(CreateInstancesNames(task.DeployDescriptor.InstancesMap()), task.OutputId, typeof(ShardedSubscribeOperator).Name.ToLower(), task, 1);
                if (status == CRAErrorCode.Success)
                {
                    foreach (string fromInputId in task.EndpointsDescriptor.FromInputs.Keys)
                    {
                        var fromToConnection = task.VerticesConnectionsMap[fromInputId + task.OutputId][0];
                        await client.Connect(fromToConnection.FromVertex, fromToConnection.FromEndpoint, fromToConnection.ToVertex, fromToConnection.ToEndpoint);
                    }
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

        public static async Task<bool> DeploySubscribeTaskOld(
            CRAClientLibrary client,
            SubscribeTask task,
            OperatorsToplogy topology)
        {
            try
            {
                await client.DefineVertex(
                    typeof(SubscribeOperator).Name.ToLower(),
                    () => new SubscribeOperator(client.DataProvider));

                CRAErrorCode status =  client.InstantiateShardedVertex(task.OutputId, typeof(SubscribeOperator).Name.ToLower(),
                                            task, task.DeployDescriptor.InstancesMap());
                if (status == CRAErrorCode.Success)
                {
                    foreach (string fromInputId in task.EndpointsDescriptor.FromInputs.Keys)
                        client.ConnectShardedVertices(task.VerticesConnectionsMap[fromInputId  + task.OutputId]);

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


        private static async Task<bool> DeployShuffleReduceTask(
            CRAClientLibrary client,
            ShuffleTask task,
            OperatorsToplogy topology)
        {
            try
            {
                await client.DefineVertex(typeof(ShardedShuffleOperator).Name.ToLower(), () => new ShardedShuffleOperator());
                var status = await client.InstantiateVertex(CreateInstancesNames(task.DeployDescriptor.InstancesMap()), task.ReducerVertexName, typeof(ShardedShuffleOperator).Name.ToLower(), task, 1);
                if (status == CRAErrorCode.Success) {
                    foreach (string fromInputId in task.SecondaryEndpointsDescriptor.FromInputs.Keys)
                    {
                        var fromToConnection = task.VerticesConnectionsMap[fromInputId + task.OutputId][0];
                        await client.Connect(fromToConnection.FromVertex, fromToConnection.FromEndpoint, fromToConnection.ToVertex, fromToConnection.ToEndpoint);
                    }

                    var fromInput = task.EndpointsDescriptor.FromInputs.First().Key;
                    var newFromInputId = fromInput.Substring(0, fromInput.Length - 1);
                    var newFromToConnection = task.VerticesConnectionsMap[newFromInputId + task.ReducerVertexName][0];
                    await client.Connect(newFromToConnection.FromVertex, newFromToConnection.FromEndpoint, newFromToConnection.ToVertex, newFromToConnection.ToEndpoint);

                    return true;
                }
                else
                    return false;
            }
            catch (Exception e)
            {
                Console.WriteLine("Error in deploying a sharded CRA shuffle mapper task. Please, double check your task configurations: " + e.ToString());
                return false;
            }
        }

        private static async Task<bool> DeployShuffleReduceTaskOld(
            CRAClientLibrary client,
            ShuffleTask task,
            OperatorsToplogy topology)
        {
            try
            {
                await client.DefineVertex(
                    typeof(ShuffleOperator).Name.ToLower(),
                    () => new ShuffleOperator(client.DataProvider));

                var status =  client.InstantiateShardedVertex(
                    task.ReducerVertexName,
                    typeof(ShuffleOperator).Name.ToLower(),
                    task,
                    task.DeployDescriptor.InstancesMap());

                if (status == CRAErrorCode.Success)
                {
                    foreach (string fromInputId in task.SecondaryEndpointsDescriptor.FromInputs.Keys)
                        client.ConnectShardedVertices(task.VerticesConnectionsMap[fromInputId + task.OutputId]);

                    var fromInput = task.EndpointsDescriptor.FromInputs.First().Key;
                    var newFromInputId = fromInput.Substring(0, fromInput.Length - 1);
                    client.ConnectShardedVertices(task.VerticesConnectionsMap[newFromInputId + task.ReducerVertexName]);

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

        public static async Task<bool> DeployClientTerminal(
            CRAClientLibrary client,
            ClientTerminalTask task,
            DetachedVertex clientTerminal,
            OperatorsToplogy topology)
        {
            try
            {
                foreach (string fromInputId in task.EndpointsDescriptor.FromInputs.Keys)
                {
                    string inputEndpoint = OperatorUtils.PrepareInputEndpointIdForOperator(fromInputId, task.EndpointsDescriptor);
                    string outputEndpoint = OperatorUtils.PrepareOutputEndpointIdForOperator(
                                    task.OutputId, topology.OperatorsEndpointsDescriptors[fromInputId]);
                    int shardsCount = client.CountVertexShards(task.DeployDescriptor.InstancesMap());
                    for (int i = 0; i < shardsCount; i++)
                            await clientTerminal.FromRemoteOutputEndpointStream(inputEndpoint + i, fromInputId + "$" + i, outputEndpoint);
                }
                return true;
            }
            catch (Exception e)
            {
                Console.WriteLine("Error in deploying a CRA client terminal vertex. Please, double check your task configurations: " + e.ToString());
                return false;
            }
        }

        public static async Task<bool> DeployClientTerminalOld(
            CRAClientLibrary client,
            ClientTerminalTask task,
            DetachedVertex clientTerminal,
            OperatorsToplogy topology)
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
                            await clientTerminal.FromRemoteOutputEndpointStream(
                                inputEndpoints[j] + i,
                                fromInputId + "$" + i,
                                outputEndpoints[j]);
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
            string output = OperatorUtils.PrepareOutputEndpointIdForOperator(toVertex, fromVertexDescriptor);
            string input = OperatorUtils.PrepareInputEndpointIdForOperator(fromVertex, toVertexDescriptor);

            bool hasSameCRAInstances = true;
            for (int i = 0; i < shardsCount; i++)
            {
                string currentFromVertex = fromVertex + "$" + i;
                string currentToVertex = toVertex + "$" + i;
                hasSameCRAInstances = hasSameCRAInstances & client.AreTwoVerticessOnSameCRAInstance(currentFromVertex, fromVertexShards, currentToVertex, toVertexShards);
            }

            fromToConnections.Add(new ConnectionInfoWithLocality(fromVertex, output, toVertex, input, hasSameCRAInstances));
            return fromToConnections;
        }

        /*
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
                bool hasSameCRAInstances = client.AreTwoVerticessOnSameCRAInstance(currentFromVertex, fromVertexShards, currentToVertex, toVertexShards);
                for (int j = 0; j < outputs.Length; j++)
                {
                    var fromVertexTuple = new Tuple<string, string>(currentFromVertex, outputs[j]);
                    var toVertexTuple = new Tuple<string, string>(currentToVertex, inputs[j]);
                    fromToConnections.Add(new ConnectionInfoWithLocality(currentFromVertex, outputs[j], currentToVertex, inputs[j], hasSameCRAInstances));
                }
            }
            return fromToConnections;
        }*/

        private static List<ConnectionInfoWithLocality> PrepareShuffleConnectionsMap(CRAClientLibrary client, int shardsCount, 
                            string fromVertex, OperatorEndpointsDescriptor fromVertexDescriptor, ConcurrentDictionary<string, int> fromVertexShards,
                            string toVertex, OperatorEndpointsDescriptor toVertexDescriptor, ConcurrentDictionary<string, int> toVertexShards)
        {
           List<ConnectionInfoWithLocality> fromToConnections = new List<ConnectionInfoWithLocality>();
            string output = OperatorUtils.PrepareOutputEndpointIdForOperator(toVertex, fromVertexDescriptor);
            string input = OperatorUtils.PrepareInputEndpointIdForOperator(fromVertex, toVertexDescriptor);

            bool hasSameCRAInstances = true;
            for (int i = 0; i < shardsCount; i++)
            {
                for (int j = 0; j < shardsCount; j++)
                {
                    string currentFromVertex = fromVertex + "$" + i;
                    string currentToVertex = toVertex + "$" + j;
                    hasSameCRAInstances = hasSameCRAInstances & client.AreTwoVerticessOnSameCRAInstance(currentFromVertex, fromVertexShards, currentToVertex, toVertexShards);
                }
            }

            fromToConnections.Add(new ConnectionInfoWithLocality(fromVertex, output, toVertex, input, hasSameCRAInstances));
            return fromToConnections; 
        }

        /*
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
                    bool hasSameCRAInstances = client.AreTwoVerticessOnSameCRAInstance(currentFromVertex, fromVertexShards, currentToVertex, toVertexShards);
                    for (int k = 0; k < currentOutputs.Length; k++)
                        fromToConnections.Add(new ConnectionInfoWithLocality(currentFromVertex, currentOutputs[k], currentToVertex, currentInputs[k], hasSameCRAInstances));
                }
            }

            return fromToConnections;
        }
        */
        private static string[] CreateInstancesNames(ConcurrentDictionary<string, int> instancesMap)
        {
            var keys = instancesMap.Keys;
            string[] instancesNames = new string[keys.Count];
            for (int i = 0; i < keys.Count; i++)
                instancesNames[i] = keys.ElementAt(i);
            return instancesNames;
        }
    }
}
