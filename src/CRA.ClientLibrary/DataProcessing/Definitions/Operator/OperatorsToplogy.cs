using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Runtime.Serialization;

namespace CRA.ClientLibrary.DataProcessing
{
    public class OperatorsToplogy
    {
        ConcurrentDictionary<string, OperatorEndpointsDescriptor> _operatorsEndpointsDescriptors;
        private List<string> _operatorsIds;
        private List<TaskBase> _operatorsTasks;

        internal ConcurrentDictionary<string, OperatorEndpointsDescriptor> OperatorsEndpointsDescriptors { get { return _operatorsEndpointsDescriptors; } }
        internal List<string> OperatorsIds { get { return _operatorsIds; } }
        internal List<TaskBase> OperatorsTasks { get { return _operatorsTasks; } }

        private static OperatorsToplogy _instance = null;

        public static OperatorsToplogy GetInstance()
        {
            if (_instance == null)
                _instance = new OperatorsToplogy();

            return _instance;
        }

        private OperatorsToplogy()
        {
            _operatorsEndpointsDescriptors = new ConcurrentDictionary<string, OperatorEndpointsDescriptor>();
            _operatorsIds = new List<string>();
            _operatorsTasks = new List<TaskBase>();
        }

        internal void AddOperatorBase(string taskId, TaskBase task)
        {
            _operatorsEndpointsDescriptors.AddOrUpdate(taskId, new OperatorEndpointsDescriptor(), (operatorId, descriptor) => new OperatorEndpointsDescriptor());
            if (_operatorsIds.Contains(taskId))
            {
                int index = _operatorsIds.IndexOf(taskId);
                _operatorsTasks[index] = task;
            }
            else
            {
                _operatorsIds.Add(taskId);
                _operatorsTasks.Add(task);
            }
        }

        internal void AddShuffleOperator(string mapperTaskId, string reducerTaskId, ShuffleTask task)
        {
            _operatorsEndpointsDescriptors.AddOrUpdate(mapperTaskId, new OperatorEndpointsDescriptor(), (operatorId, descriptor) => new OperatorEndpointsDescriptor());
            _operatorsEndpointsDescriptors.AddOrUpdate(reducerTaskId, new OperatorEndpointsDescriptor(), (operatorId, descriptor) => new OperatorEndpointsDescriptor());
            if (_operatorsIds.Contains(reducerTaskId))
            {
                int index = _operatorsIds.IndexOf(reducerTaskId);
                _operatorsTasks[index] = task;
            }
            else
            {
                _operatorsIds.Add(reducerTaskId);
                _operatorsTasks.Add(task);
            }
        }

        internal void UpdateShuffleOperatorTask(string taskId, ShuffleTask task)
        {
            if (_operatorsIds.Contains(taskId))
            {
                int index = _operatorsIds.IndexOf(taskId);
                _operatorsTasks[index] = task;
            }
        }

        internal void AddOperatorInput(string taskId, string fromInputId, int inputsCount = 1)
        {
            if (taskId != null && taskId != "$" && fromInputId != null && fromInputId != "$")
            {
                OperatorEndpointsDescriptor operatorDescriptor = _operatorsEndpointsDescriptors[taskId];
                operatorDescriptor.FromInputs.AddOrUpdate(fromInputId, inputsCount, (inputId, currentInputs) => inputsCount);
                _operatorsEndpointsDescriptors[taskId] = operatorDescriptor;
            }
        }

        internal void AddOperatorSecondaryInput(string taskId, string secondaryFromInputId, int inputsCount = 1)
        {
            if (taskId != null && taskId != "$" && secondaryFromInputId != null && secondaryFromInputId != "$")
            {
                OperatorEndpointsDescriptor operatorDescriptor = _operatorsEndpointsDescriptors[taskId];
                operatorDescriptor.SecondaryFromInputs.AddOrUpdate(secondaryFromInputId, inputsCount, (inputId, currentInputs) => inputsCount);
                _operatorsEndpointsDescriptors[taskId] = operatorDescriptor;
            }
        }

        private int RemoveOperatorInput(string taskId, string fromInputId)
        {
            int updatedInputs = 0;
            if (taskId != null && fromInputId != null)
            {
                OperatorEndpointsDescriptor operatorDescriptor = _operatorsEndpointsDescriptors[taskId];
                operatorDescriptor.FromInputs.TryRemove(fromInputId, out updatedInputs);
                _operatorsEndpointsDescriptors[taskId] = operatorDescriptor;
            }
            return updatedInputs;
        }

        internal void AddOperatorOutput(string taskId, string toOutputId, int outputsCount = 1)
        {
            if (taskId != null && taskId != "$" && toOutputId != null && toOutputId != "$")
            {
                OperatorEndpointsDescriptor operatorDescriptor = _operatorsEndpointsDescriptors[taskId];
                operatorDescriptor.ToOutputs.AddOrUpdate(toOutputId, outputsCount, (outputId, currentOutputs) => outputsCount);
                _operatorsEndpointsDescriptors[taskId] = operatorDescriptor;
            }
        }

        private int RemoveOperatorOutput(string taskId, string toOutputId)
        {
            int updatedOutputs = 0;
            if (taskId != null && toOutputId != null)
            {
                OperatorEndpointsDescriptor operatorDescriptor = _operatorsEndpointsDescriptors[taskId];
                operatorDescriptor.ToOutputs.TryRemove(toOutputId, out updatedOutputs);
                _operatorsEndpointsDescriptors[taskId] = operatorDescriptor;
            }
            return updatedOutputs;
        }

        internal void UpdateShuffleInputs(string mapperId, string reducerId, int shardingCount)
        {
            if (mapperId != null && reducerId != null)
            {
                var mapperDescriptor = _operatorsEndpointsDescriptors[mapperId];
                var mapperInputs = mapperDescriptor.FromInputs;
                foreach (string inputId in mapperInputs.Keys)
                {
                    int outputEndpointsCount = RemoveOperatorOutput(inputId, mapperId);
                    for (int i = 0; i < shardingCount; i++)
                        AddOperatorOutput(inputId, reducerId + i, outputEndpointsCount);

                    int inputEndpointsCount = RemoveOperatorInput(mapperId, inputId);
                    for (int i = 0; i < shardingCount; i++)
                        AddOperatorInput(reducerId, inputId + i, inputEndpointsCount);
                }
            }
        }

        private void UpdateOperatorsSecondaryInputs(string firstOperatorId, string secondOperatorId)
        {
            OperatorEndpointsDescriptor secondOperatorDescriptor = _operatorsEndpointsDescriptors[secondOperatorId];
            var secondaryInputs = secondOperatorDescriptor.SecondaryFromInputs;
            foreach (string inputId  in secondaryInputs.Keys)
            {
                RemoveOperatorOutput(inputId, secondOperatorId);
                AddOperatorInput(firstOperatorId, inputId, secondaryInputs[inputId]);
                AddOperatorOutput(inputId, firstOperatorId, secondaryInputs[inputId]);
            }
        }

        private void UpdateOperatorsSecondaryInput(string firstOperatorId, string secondOperatorId, string secondaryInputId)
        {
            OperatorEndpointsDescriptor secondOperatorDescriptor = _operatorsEndpointsDescriptors[secondOperatorId];
            var secondaryInputs = secondOperatorDescriptor.SecondaryFromInputs;

            var secondOperatorInputs = secondOperatorDescriptor.FromInputs;
            if (!secondOperatorInputs.ContainsKey(secondaryInputId))
                RemoveOperatorOutput(secondaryInputId, secondOperatorId);
            AddOperatorInput(firstOperatorId, secondaryInputId, secondaryInputs[secondaryInputId]);
            AddOperatorOutput(secondaryInputId, firstOperatorId, secondaryInputs[secondaryInputId]);
        }

        private int RetrieveTaskIndexOfOperator(string operatorId, string[] operatorsIds)
        {
            for (int i = 0; i < operatorsIds.Length; i++)
            {
                if (operatorsIds[i].Equals(operatorId))
                    return i;
            }

            throw new InvalidOperationException();
        }

        internal void PrepareFinalOperatorsTasks()
        {
            string[] operatorsIds = _operatorsIds.ToArray();
            TaskBase[] tasks =  _operatorsTasks.ToArray();

            List<string>[] tmpTransforms = new List<string>[tasks.Length];
            List<string>[] tmpTransformsOperations = new List<string>[tasks.Length];
            List<string>[] tmpTransformsTypes = new List<string>[tasks.Length];
            List<OperatorInputs>[] tmpTransformsInputs = new List<OperatorInputs>[tasks.Length];

            for(int i = 0; i < tasks.Length; i++)
            {
                tmpTransforms[i] = new List<string>();
                tmpTransformsOperations[i] = new List<string>();
                tmpTransformsTypes[i] = new List<string>();
                tmpTransformsInputs[i] = new List<OperatorInputs>();
            }

            if (tasks.Length >= 2)
            {
                for (int i = 0; i < operatorsIds.Length; i++)
                {
                    if (tasks[i].Transforms != null)
                    {
                        int lastProcessedTransformIndex = 0;
                        for (int j = 0; j < tasks[i].Transforms.Length; j++)
                        {
                            var currentInput = tasks[i].TransformsInputs[j].InputId1;
                            int inputTaskIndex = RetrieveTaskIndexOfOperator(currentInput, operatorsIds);

                            if (tmpTransforms[inputTaskIndex].Count > 0 &&
                                tmpTransformsOperations[inputTaskIndex][tmpTransforms[inputTaskIndex].Count - 1] == OperatorType.MoveSplit.ToString())
                            {
                                break;
                            }
                            else
                            {
                                tmpTransforms[inputTaskIndex].Add(tasks[i].Transforms[j]);
                                tmpTransformsOperations[inputTaskIndex].Add(tasks[i].TransformsOperations[j]);
                                tmpTransformsTypes[inputTaskIndex].Add(tasks[i].TransformsTypes[j]);
                                tmpTransformsInputs[inputTaskIndex].Add(tasks[i].TransformsInputs[j]);

                                if (tasks[i].TransformsOperations[j] == OperatorType.BinaryTransform.ToString())
                                {
                                    var currentSecondaryInput = tasks[i].TransformsInputs[j].InputId2;

                                    if (tasks[inputTaskIndex].OperationType != OperatorType.Move && tasks[i].OperationType != OperatorType.Move)
                                        UpdateOperatorsSecondaryInput(operatorsIds[inputTaskIndex], operatorsIds[i], currentSecondaryInput);
                                    else if (tasks[inputTaskIndex].OperationType != OperatorType.Move && tasks[i].OperationType == OperatorType.Move)
                                        UpdateOperatorsSecondaryInput(operatorsIds[inputTaskIndex], ((ShuffleTask)tasks[i]).MapperVertexName, currentSecondaryInput);
                                    else if (tasks[inputTaskIndex].OperationType == OperatorType.Move && tasks[i].OperationType != OperatorType.Move)
                                        UpdateOperatorsSecondaryInput(((ShuffleTask)tasks[inputTaskIndex]).ReducerVertexName, operatorsIds[i], currentSecondaryInput);
                                    else
                                        UpdateOperatorsSecondaryInput(((ShuffleTask)tasks[inputTaskIndex]).ReducerVertexName, ((ShuffleTask)tasks[i]).MapperVertexName, currentSecondaryInput);
                                }

                                lastProcessedTransformIndex++;
                            }
                        }

                        for (int k = lastProcessedTransformIndex; k < tasks[i].Transforms.Length; k++)
                        {
                            tmpTransforms[i].Add(tasks[i].Transforms[k]);
                            tmpTransformsOperations[i].Add(tasks[i].TransformsOperations[k]);
                            tmpTransformsTypes[i].Add(tasks[i].TransformsTypes[k]);
                            tmpTransformsInputs[i].Add(tasks[i].TransformsInputs[k]);
                        }
                    }

                    if (tasks[i].OperationType == OperatorType.Move)
                    {
                        var currentInput = ((ShuffleTask)tasks[i]).ShuffleTransformsInputs[0].InputId1;
                        int inputTaskIndex = RetrieveTaskIndexOfOperator(currentInput, operatorsIds);

                        if (!(tmpTransforms[inputTaskIndex].Count > 0 &&
                            tmpTransformsOperations[inputTaskIndex][tmpTransforms[inputTaskIndex].Count - 1] == OperatorType.MoveSplit.ToString()))
                        {
                            tmpTransforms[inputTaskIndex].Add(((ShuffleTask)tasks[i]).ShuffleTransforms[0]);
                            tmpTransformsOperations[inputTaskIndex].Add(((ShuffleTask)tasks[i]).ShuffleTransformsOperations[0]);
                            tmpTransformsTypes[inputTaskIndex].Add(((ShuffleTask)tasks[i]).ShuffleTransformsTypes[0]);
                            tmpTransformsInputs[inputTaskIndex].Add(((ShuffleTask)tasks[i]).ShuffleTransformsInputs[0]);
                            tasks[inputTaskIndex].SecondaryShuffleDescriptor = ((ShuffleTask)tasks[i]).ShuffleDescriptor;
                        }

                    }
                }

                for (int i = 0; i < tasks.Length; i++)
                {
                    tasks[i].Transforms = tmpTransforms[i].ToArray();
                    tasks[i].TransformsOperations = tmpTransformsOperations[i].ToArray();
                    tasks[i].TransformsTypes = tmpTransformsTypes[i].ToArray();
                    tasks[i].TransformsInputs = tmpTransformsInputs[i].ToArray();
                }

                _operatorsTasks = new List<TaskBase>(tasks);
            }
        }
    }

    [Serializable, DataContract]
    public class OperatorEndpointsDescriptor
    {
        [DataMember]
        private ConcurrentDictionary<string, int> _fromInputs;

        [DataMember]
        private ConcurrentDictionary<string, int> _secondaryFromInputs;

        [DataMember]
        private ConcurrentDictionary<string, int> _toOutputs;

        public ConcurrentDictionary<string, int> FromInputs
        {
            get { return _fromInputs; }
            set { _fromInputs = value; }
        }

        public ConcurrentDictionary<string, int> SecondaryFromInputs
        {
            get { return _secondaryFromInputs; }
        }

        public ConcurrentDictionary<string, int> ToOutputs
        {
            get { return _toOutputs; }
        }
        
        public OperatorEndpointsDescriptor()
        {
            _fromInputs = new ConcurrentDictionary<string, int> ();
            _secondaryFromInputs = new ConcurrentDictionary<string, int>();
            _toOutputs = new ConcurrentDictionary<string, int>();
        }
    }
}
