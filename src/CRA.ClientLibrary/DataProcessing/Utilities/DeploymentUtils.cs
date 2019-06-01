using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading;
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
        private static bool _isProduceOperatorDefined = false;
        private static bool _isShuffleOperatorDefined = false;
        private static bool _isSubscribeOperatorDefined = false;
        private static bool _isSubscribeClientOperatorDefined = false;

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
            var tasksDeploymentStatus = new Dictionary<string, bool>();
            for (int i = 0; i < tasks.Length; i++)
            {
                tasks[i].EndpointsDescriptor = topology.OperatorsEndpointsDescriptors[tasksIds[i]];
                tasks[i].VerticesConnectionsMap = connectionsMap;
                tasksDeploymentStatus.Add(tasksIds[i], false);
            }

            bool isSuccessful = true;
            for (int i = 0; i < tasks.Length; i++)
            {
                if (tasks[i].OperationType == OperatorType.Produce && tasks[i].EndpointsDescriptor.FromInputs.Count == 0 
                    && tasks[i].EndpointsDescriptor.SecondaryFromInputs.Count == 0)
                {
                    isSuccessful = await DeployProduceTask(client, (ProduceTask)tasks[i], topology);
                    if (isSuccessful) tasksDeploymentStatus[tasksIds[i]] = true;
                }
            }

            for (int i = 0; i < tasks.Length; i++)
                isSuccessful = isSuccessful & await DeployTask(i, tasks, tasksIds, tasksDeploymentStatus, client, topology);
            
            return isSuccessful;
        }

        private static async Task<bool> DeployTask(int taskIndex, TaskBase[] tasks, string[] tasksIds, Dictionary<string, bool> tasksDeploymentStatus, CRAClientLibrary client, OperatorsToplogy topology)
        {
            if (!tasksDeploymentStatus[tasksIds[taskIndex]])
            {
                bool isSuccessful = true;
                foreach (var fromInput in tasks[taskIndex].EndpointsDescriptor.FromInputs.Keys)
                {
                    int fromInputIndex = RetrieveTaskIndexOfOperator(fromInput, tasksIds);
                    isSuccessful = isSuccessful & await DeployTask(fromInputIndex, tasks, tasksIds, tasksDeploymentStatus, client, topology);
                }
                foreach (var fromSecondaryInput in tasks[taskIndex].EndpointsDescriptor.SecondaryFromInputs.Keys)
                {
                    int fromSecondaryInputIndex = RetrieveTaskIndexOfOperator(fromSecondaryInput, tasksIds);
                    isSuccessful = isSuccessful & await DeployTask(fromSecondaryInputIndex, tasks, tasksIds, tasksDeploymentStatus, client, topology);
                }

                if (isSuccessful)
                {
                    if (tasks[taskIndex].OperationType == OperatorType.Produce)
                    {
                        isSuccessful = isSuccessful & await DeployProduceTask(client, (ProduceTask)tasks[taskIndex], topology);
                        if (isSuccessful) tasksDeploymentStatus[tasksIds[taskIndex]] = true;
                    }
                    else if (tasks[taskIndex].OperationType == OperatorType.Subscribe)
                    {
                        isSuccessful = isSuccessful & await DeploySubscribeTask(client, (SubscribeTask)tasks[taskIndex], topology);
                        if (isSuccessful) tasksDeploymentStatus[tasksIds[taskIndex]] = true;
                    }
                    else if (tasks[taskIndex].OperationType == OperatorType.Move)
                    {
                        isSuccessful = isSuccessful & await DeployShuffleReduceTask(client, (ShuffleTask)tasks[taskIndex], topology);
                        if (isSuccessful) tasksDeploymentStatus[tasksIds[taskIndex]] = true;
                    }
                }
                return isSuccessful;
            }
            else
                return true;
        }

        internal static int RetrieveTaskIndexOfOperator(string operatorId, string[] operatorsIds)
        {
            for (int i = 0; i < operatorsIds.Length; i++)
            {
                if (operatorsIds[i].Equals(operatorId))
                    return i;
            }

            throw new InvalidOperationException();
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
                foreach (string fromInputId in tasks[i].EndpointsDescriptor.FromInputs.Keys)
                {
                    var flatFromToConnections = PrepareFlatConnectionsMap(client, shardsCount,
                                fromInputId, topology.OperatorsEndpointsDescriptors[fromInputId], tasksDictionary[fromInputId].DeployDescriptor.InstancesMap(),
                                tasks[i].OutputId, tasks[i].EndpointsDescriptor, tasks[i].DeployDescriptor.InstancesMap(), false);
                    verticesConnectionsMap.AddOrUpdate(fromInputId + tasks[i].OutputId,
                                            flatFromToConnections, (key, value) => flatFromToConnections);
                }

                foreach (string secondaryFromInputId in tasks[i].EndpointsDescriptor.SecondaryFromInputs.Keys)
                {
                    var flatFromToConnections = PrepareFlatConnectionsMap(client, shardsCount,
                                secondaryFromInputId, topology.OperatorsEndpointsDescriptors[secondaryFromInputId], tasksDictionary[secondaryFromInputId].DeployDescriptor.InstancesMap(),
                                tasks[i].OutputId, tasks[i].EndpointsDescriptor, tasks[i].DeployDescriptor.InstancesMap(), true);
                    verticesConnectionsMap.AddOrUpdate(secondaryFromInputId + tasks[i].OutputId,
                                            flatFromToConnections, (key, value) => flatFromToConnections);
                }
                
            }
            return verticesConnectionsMap;
        }

        public static async Task<bool> DeployProduceTask(CRAClientLibrary client, ProduceTask task, OperatorsToplogy topology) {
            try
            {
                if (!_isProduceOperatorDefined)
                {
                    await client.DefineVertex(typeof(ShardedProducerOperator).Name.ToLower(), () => new ShardedProducerOperator());
                    _isProduceOperatorDefined = true;
                }

                var status = await client.InstantiateVertex(CreateInstancesNames(task.DeployDescriptor.InstancesMap()), task.OutputId, typeof(ShardedProducerOperator).Name.ToLower(), task, 1);
                if (status == CRAErrorCode.Success)
                {
                    foreach (string fromSecondaryInputId in task.EndpointsDescriptor.SecondaryFromInputs.Keys)
                    {
                        var fromToConnection = task.VerticesConnectionsMap[fromSecondaryInputId + task.OutputId][0];
                        await client.Connect(fromToConnection.FromVertex, fromToConnection.FromEndpoint, fromToConnection.ToVertex, fromToConnection.ToEndpoint);
                    }
                    return true;
                }
                else
                    return false;
            }
            catch (Exception e)
            {
                Console.WriteLine("Error in deploying a sharded CRA produce task. Please, double check your task configurations: " + e.ToString());
                return false;
            }
        }
     
        public static async Task<bool> DeploySubscribeTask(CRAClientLibrary client, SubscribeTask task, OperatorsToplogy topology)
        {
            try
            {
                if (!_isSubscribeOperatorDefined)
                {
                    await client.DefineVertex(typeof(ShardedSubscribeOperator).Name.ToLower(), () => new ShardedSubscribeOperator());
                    _isSubscribeOperatorDefined = true;
                }

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

        private static async Task<bool> DeployShuffleReduceTask(
            CRAClientLibrary client,
            ShuffleTask task,
            OperatorsToplogy topology)
        {
            try
            {
                if (!_isShuffleOperatorDefined)
                {
                    await client.DefineVertex(typeof(ShardedShuffleOperator).Name.ToLower(), () => new ShardedShuffleOperator());
                    _isShuffleOperatorDefined = true;
                }

                var status = await client.InstantiateVertex(CreateInstancesNames(task.DeployDescriptor.InstancesMap()), task.ReducerVertexName, typeof(ShardedShuffleOperator).Name.ToLower(), task, 1);
                if (status == CRAErrorCode.Success) {

                    foreach (string fromInputId in task.EndpointsDescriptor.FromInputs.Keys)
                    {
                        var fromToConnection = task.VerticesConnectionsMap[fromInputId + task.ReducerVertexName][0];
                        await client.Connect(fromToConnection.FromVertex, fromToConnection.FromEndpoint, fromToConnection.ToVertex, fromToConnection.ToEndpoint);
                    }

                    foreach (string fromSecondaryInputId in task.EndpointsDescriptor.SecondaryFromInputs.Keys)
                    {
                        var fromToConnection = task.VerticesConnectionsMap[fromSecondaryInputId + task.ReducerVertexName][0];
                        await client.Connect(fromToConnection.FromVertex, fromToConnection.FromEndpoint, fromToConnection.ToVertex, fromToConnection.ToEndpoint);
                    }
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

        public static async Task<bool> DeployClientTerminal(
            CRAClientLibrary client,
            string workerName,
            ClientTerminalTask task,
            OperatorsToplogy topology)
        {
            try
            {
                bool result = true;

                client.DisableArtifactUploading();

                if (!_isSubscribeClientOperatorDefined)
                {
                    await client.DefineVertex(typeof(ShardedSubscribeClientOperator).Name.ToLower(), () => new ShardedSubscribeClientOperator());
                    _isSubscribeClientOperatorDefined = true;
                }

                var status = await client.InstantiateVertex(new string[] {workerName}, task.OutputId, typeof(ShardedSubscribeClientOperator).Name.ToLower(), task, 1);

                if (status == CRAErrorCode.Success)
                {
                    foreach (string fromInputId in task.EndpointsDescriptor.FromInputs.Keys)
                    {
                        string outputEndpoint = OperatorUtils.PrepareOutputEndpointIdForOperator(
                                        task.OutputId, topology.OperatorsEndpointsDescriptors[fromInputId]);
                        string inputEndpoint = OperatorUtils.PrepareInputEndpointIdForOperator(fromInputId, task.EndpointsDescriptor, false);

                        await client.Connect(fromInputId, outputEndpoint, task.OutputId, inputEndpoint);
                    }
                    result = true;
                }
                else
                    result = false;

                client.EnableArtifactUploading();

                return result;
            }
            catch (Exception e)
            {
                Console.WriteLine("Error in deploying a CRA client terminal vertex. Please, double check your task configurations: " + e.ToString());
                return false;
            }
        }

        private static List<ConnectionInfoWithLocality> PrepareFlatConnectionsMap(CRAClientLibrary client, int shardsCount,
                            string fromVertex, OperatorEndpointsDescriptor fromVertexDescriptor, ConcurrentDictionary<string, int> fromVertexShards,
                            string toVertex, OperatorEndpointsDescriptor toVertexDescriptor, ConcurrentDictionary<string, int> toVertexShards, bool isSecondaryInput)
        {
            List<ConnectionInfoWithLocality> fromToConnections = new List<ConnectionInfoWithLocality>();
            string output = OperatorUtils.PrepareOutputEndpointIdForOperator(toVertex, fromVertexDescriptor);
            string input = OperatorUtils.PrepareInputEndpointIdForOperator(fromVertex, toVertexDescriptor, isSecondaryInput);

            bool hasSameCRAInstances = true;
            for (int i = 0; i < shardsCount; i++)
            {
                string currentFromVertex = fromVertex + "$" + i;
                string currentToVertex = toVertex + "$" + i;
                hasSameCRAInstances = hasSameCRAInstances & client.AreTwoVerticessOnSameCRAInstance(currentFromVertex, fromVertexShards, currentToVertex, toVertexShards);
            }

            fromToConnections.Add(new ConnectionInfoWithLocality(fromVertex, output, toVertex, input, hasSameCRAInstances, isSecondaryInput));
            return fromToConnections;
        }

        private static List<ConnectionInfoWithLocality> PrepareShuffleConnectionsMap(CRAClientLibrary client, int shardsCount, 
                            string fromVertex, OperatorEndpointsDescriptor fromVertexDescriptor, ConcurrentDictionary<string, int> fromVertexShards,
                            string toVertex, OperatorEndpointsDescriptor toVertexDescriptor, ConcurrentDictionary<string, int> toVertexShards, bool isSecondaryInput)
        {
           List<ConnectionInfoWithLocality> fromToConnections = new List<ConnectionInfoWithLocality>();
            string output = OperatorUtils.PrepareOutputEndpointIdForOperator(toVertex, fromVertexDescriptor);
            string input = OperatorUtils.PrepareInputEndpointIdForOperator(fromVertex, toVertexDescriptor, isSecondaryInput);

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

            fromToConnections.Add(new ConnectionInfoWithLocality(fromVertex, output, toVertex, input, hasSameCRAInstances, isSecondaryInput));
            return fromToConnections; 
        }

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
