using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading;

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
        public static IDeployDescriptor CreateDefaultDeployDescriptor()
        {
            ConcurrentDictionary<string, int> deployShards = new ConcurrentDictionary<string, int>();
            deployShards.AddOrUpdate("crainst01", 1, (inst, proc) => 1);
            deployShards.AddOrUpdate("crainst02", 1, (inst, proc) => 1);
            return new DeployDescriptorBase(deployShards);
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

            bool isSuccessful = true;
            for (int i = 0; i < tasks.Length; i++)
            {
                tasks[i].EndpointsDescriptor = topology.OperatorsEndpointsDescriptors[tasksIds[i]];
                
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
                    int shardsCount = client.CountProcessShards(task.DeployDescriptor.InstancesMap());
                    foreach (string fromInputId in task.EndpointsDescriptor.FromInputs.Keys)
                    {
                        var flatFromToConnections = PrepareFlatConnectionsMap(shardsCount,
                                    fromInputId, topology.OperatorsEndpointsDescriptors[fromInputId],
                                    task.OutputId, task.EndpointsDescriptor);
                        client.ConnectShardedProcesses(flatFromToConnections);
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

        private static bool DeployShuffleReduceTask(CRAClientLibrary client, ShuffleTask task, OperatorsToplogy topology)
        {
            try
            {
                client.DefineProcess(typeof(ShuffleOperator).Name.ToLower(), () => new ShuffleOperator());
                CRAErrorCode status =  client.InstantiateShardedProcess(task.ReducerProcessName, typeof(ShuffleOperator).Name.ToLower(),
                                                task, task.DeployDescriptor.InstancesMap());
                if (status == CRAErrorCode.Success)
                {
                    int shardsCount = client.CountProcessShards(task.DeployDescriptor.InstancesMap());
                    foreach (string fromInputId in task.SecondaryEndpointsDescriptor.FromInputs.Keys)
                    {
                        var flatFromToConnections = PrepareFlatConnectionsMap(shardsCount,
                                    fromInputId, topology.OperatorsEndpointsDescriptors[fromInputId],
                                    task.OutputId, task.EndpointsDescriptor);
                        client.ConnectShardedProcesses(flatFromToConnections);
                    }

                    var fromInput = task.EndpointsDescriptor.FromInputs.First().Key;
                    var newFromInputId = fromInput.Substring(0, fromInput.Length - 1);
                    var shuffleFromToConnections = PrepareShuffleConnectionsMap(shardsCount,
                                                    newFromInputId, topology.OperatorsEndpointsDescriptors[newFromInputId],
                                                    task.ReducerProcessName, task.EndpointsDescriptor);
                    client.ConnectShardedProcesses(shuffleFromToConnections);

                    return true;
                }
                else
                    return false;
            }
            catch(Exception e)
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
                    Thread.Sleep(1000);
                }
                return true;
            }
            catch (Exception e)
            {
                Console.WriteLine("Error in deploying a CRA client terminal process. Please, double check your task configurations: " + e.ToString());
                return false;
            }
        }

        private static ConcurrentDictionary<Tuple<string, string>, Tuple<string, string>> PrepareFlatConnectionsMap(
                            int shardsCount, string fromProcess, OperatorEndpointsDescriptor fromProcessDescriptor,
                            string toProcess, OperatorEndpointsDescriptor toProcessDescriptor)
        {
            ConcurrentDictionary<Tuple<string, string>, Tuple<string, string>> fromToConnections 
                            = new ConcurrentDictionary<Tuple<string, string>, Tuple<string, string>>();
            string[] outputs = OperatorUtils.PrepareOutputEndpointsIdsForOperator(
                                                            toProcess, fromProcessDescriptor);
            string[] inputs = OperatorUtils.PrepareInputEndpointsIdsForOperator(
                                                            fromProcess, toProcessDescriptor);
            for (int i = 0; i < shardsCount; i++)
            {
                string currentFromProcess = fromProcess + "$" + i;
                string currentToProcess = toProcess + "$" + i;
                for (int j = 0; j < outputs.Length; j++)
                {
                    var fromProcessTuple = new Tuple<string, string>(currentFromProcess, outputs[j]);
                    var toProcessTuple = new Tuple<string, string>(currentToProcess, inputs[j]);
                    fromToConnections.AddOrUpdate(fromProcessTuple, toProcessTuple, (fromTuple, toTuple) => toProcessTuple);
                }
            }
            return fromToConnections;
        }

        private static ConcurrentDictionary<Tuple<string, string>, Tuple<string, string>> PrepareShuffleConnectionsMap(
                            int shardsCount, string fromProcess, OperatorEndpointsDescriptor fromProcessDescriptor, 
                            string toProcess, OperatorEndpointsDescriptor toProcessDescriptor)
        {
            ConcurrentDictionary<Tuple<string, string>, Tuple<string, string>> fromToConnections
                            = new ConcurrentDictionary<Tuple<string, string>, Tuple<string, string>>();
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
                    for (int k = 0; k < currentOutputs.Length; k++)
                    {
                        var fromProcessTuple = new Tuple<string, string>(currentFromProcess, currentOutputs[k]);
                        var toProcessTuple = new Tuple<string, string>(currentToProcess, currentInputs[k]);
                        fromToConnections.AddOrUpdate(fromProcessTuple, toProcessTuple, (fromTuple, toTuple) => toProcessTuple);
                    }
                }
            }

            return fromToConnections;
        }
    }
}
