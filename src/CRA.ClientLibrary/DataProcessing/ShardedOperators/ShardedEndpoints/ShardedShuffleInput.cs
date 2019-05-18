using System;
using System.IO;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CRA.ClientLibrary.DataProcessing
{
    public class ShardedShuffleInput : ShardedOperatorInputBase
    {
        private ShardedShuffleOperator _vertex;

        private CountdownEvent _applyMergingOnAllInputs;
        private CountdownEvent _finishMergingFromAllInputs;

        public ShardedShuffleInput(IVertex vertex, int shardId, int numOtherOperatorShards, string endpointName) : base(shardId, numOtherOperatorShards, endpointName)
        {
            _vertex = (ShardedShuffleOperator)vertex;

            _applyMergingOnAllInputs = new CountdownEvent(numOtherOperatorShards);
            _finishMergingFromAllInputs = new CountdownEvent(1);
        }

        public override async Task OperatorInputFromStreamAsync(Stream stream, string otherVertex, int otherShardId, string otherEndpoint, CancellationToken token)
        {
            _startReceivingFromOtherOperatorShards.Signal();
            _startReceivingFromOtherOperatorShards.Wait();

            // Start deploying
            _vertex._deployShuffleInput.Wait();

            await stream.WriteAsync(_deployMsgBuffer, 0, _deployMsgBuffer.Length);
            await stream.ReadAllRequiredBytesAsync(_deployMsgBuffer, 0, _deployMsgBuffer.Length);
            if (Encoding.ASCII.GetString(_deployMsgBuffer).Equals("DEPLOY"))
            {
                _vertex._deployShuffleOutput.Signal();

                // Start running
                _vertex._runShuffleInput.Wait();

                await stream.WriteAsync(_runMsgBuffer, 0, _runMsgBuffer.Length);

                var shuffleTask = (ShuffleTask)(_vertex._task);
                BinaryOperatorTypes mergeTypes = new BinaryOperatorTypes();
                mergeTypes.FromString(shuffleTask.ShuffleTransformsTypes[1]);

                _vertex._inputSplitDatasets[_shardId][otherShardId] = CreateDatasetFromStream(stream, mergeTypes.SecondaryKeyType, 
                                                                        mergeTypes.SecondaryPayloadType, mergeTypes.SecondaryDatasetType);

                _applyMergingOnAllInputs.Signal();
                _applyMergingOnAllInputs.Wait();

                if (otherShardId == 0)
                {
                    _vertex._cachedDatasets[_shardId][shuffleTask.OutputId] = ApplyMergerOnInputs(shuffleTask, mergeTypes, _vertex._inputSplitDatasets[_shardId]);
                    _vertex._outputKeyType = mergeTypes.OutputKeyType;
                    _vertex._outputPayloadType = mergeTypes.OutputPayloadType;
                    _vertex._outputDatasetType = mergeTypes.OutputDatasetType;
                    _vertex._outputId = shuffleTask.OutputId;

                    ApplyTransformersOnInputs();

                    _finishMergingFromAllInputs.Signal();
                }

                _finishMergingFromAllInputs.Wait();

                _vertex._runShuffleOutput.Signal();
            }

            _finishReceivingFromOtherOperatorShards.Signal();
            _finishReceivingFromOtherOperatorShards.Wait();
        }

        private object ApplyMergerOnInputs(ShuffleTask shuffleTask, BinaryOperatorTypes mergeTypes, object[] inputSplitDatasets)
        {
            MethodInfo method = typeof(MoveUtils).GetMethod("ApplyMerger");
            MethodInfo generic = method.MakeGenericMethod(
                new Type[] {mergeTypes.SecondaryKeyType, mergeTypes.SecondaryPayloadType,
                            mergeTypes.SecondaryDatasetType, mergeTypes.OutputKeyType,
                            mergeTypes.OutputPayloadType, mergeTypes.OutputDatasetType
                });
            object[] arguments = new Object[] { inputSplitDatasets, shuffleTask.ShuffleDescriptor, shuffleTask.ShuffleTransforms[1] };
            return generic.Invoke(this, arguments);
        }

        
        private void ApplyTransformersOnInputs()
        {
            if (_vertex._task.Transforms != null)
            {
                MethodInfo method = null; MethodInfo generic = null; object[] arguments = null;

                for (int i = 0; i < _vertex._task.Transforms.Length; i++)
                {
                    object dataset1 = null; string dataset1Id = null;
                    object dataset2 = null; string dataset2Id = null;
                    TransformUtils.PrepareTransformInputs(_vertex._task.TransformsInputs[i], ref dataset1, ref dataset1Id,
                                        ref dataset2, ref dataset2Id, _vertex._cachedDatasets[_shardId]);

                    string transformType = _vertex._task.TransformsOperations[i];
                    object transformOutput = null;
                    if (transformType == OperatorType.UnaryTransform.ToString())
                    {
                        UnaryOperatorTypes unaryTransformTypes = new UnaryOperatorTypes();
                        unaryTransformTypes.FromString(_vertex._task.TransformsTypes[i]);
                        if (dataset1Id == "$" && dataset1 == null)
                            throw new InvalidOperationException();

                        method = typeof(TransformUtils).GetMethod("ApplyUnaryTransformer");
                        generic = method.MakeGenericMethod(
                                new Type[] { unaryTransformTypes.InputKeyType, unaryTransformTypes.InputPayloadType,
                                        unaryTransformTypes.InputDatasetType, unaryTransformTypes.OutputKeyType,
                                        unaryTransformTypes.OutputPayloadType, unaryTransformTypes.OutputDatasetType
                                });
                        arguments = new Object[] { dataset1, _vertex._task.Transforms[i] };

                        _vertex._outputKeyType = unaryTransformTypes.OutputKeyType;
                        _vertex._outputPayloadType = unaryTransformTypes.OutputPayloadType;
                        _vertex._outputDatasetType = unaryTransformTypes.OutputDatasetType;
                    }
                    else if (transformType == OperatorType.BinaryTransform.ToString())
                    {
                        //TODO: to be revisited
                        /*
                        BinaryOperatorTypes binaryTransformTypes = new BinaryOperatorTypes();
                        binaryTransformTypes.FromString(_vertex._task.TransformsTypes[i]);
                        if (dataset1Id == "$" && dataset1 == null)
                            throw new InvalidOperationException();
                        if (dataset2Id == "$" && dataset2 == null)
                        {
                            dataset2Id = _vertex._task.TransformsInputs[i].InputId2;
                            dataset2 = CreateDatasetFromSecondaryInput(dataset2Id, binaryTransformTypes.SecondaryKeyType,
                                                                binaryTransformTypes.SecondaryPayloadType, binaryTransformTypes.SecondaryDatasetType);
                            if (!_vertex._cachedDatasets[_shardId].ContainsKey(dataset2Id))
                                _vertex._cachedDatasets[_shardId].Add(dataset2Id, dataset2);
                            else
                                _vertex._cachedDatasets[_shardId][dataset2Id] = dataset2;
                        }

                        method = typeof(TransformUtils).GetMethod("ApplyBinaryTransformer");
                        generic = method.MakeGenericMethod(
                            new Type[] {binaryTransformTypes.InputKeyType, binaryTransformTypes.InputPayloadType,
                                binaryTransformTypes.InputDatasetType, binaryTransformTypes.SecondaryKeyType,
                                binaryTransformTypes.SecondaryPayloadType, binaryTransformTypes.SecondaryDatasetType,
                                binaryTransformTypes.OutputKeyType, binaryTransformTypes.OutputPayloadType,
                                binaryTransformTypes.OutputDatasetType
                            });
                        arguments = new Object[] { dataset1, dataset2, _vertex._task.Transforms[i] };

                        _vertex._outputKeyType = binaryTransformTypes.OutputKeyType;
                        _vertex._outputPayloadType = binaryTransformTypes.OutputPayloadType;
                        _vertex._outputDatasetType = binaryTransformTypes.OutputDatasetType;
                        */
                    }
                    else if (transformType == OperatorType.MoveSplit.ToString())
                    {
                        BinaryOperatorTypes splitTypes = new BinaryOperatorTypes();
                        splitTypes.FromString(_vertex._task.TransformsTypes[i]);
                        if (dataset1Id == "$" && dataset1 == null)
                            throw new InvalidOperationException();

                        method = typeof(MoveUtils).GetMethod("ApplySplitter");
                        generic = method.MakeGenericMethod(
                            new Type[] {splitTypes.InputKeyType, splitTypes.InputPayloadType,
                                    splitTypes.InputDatasetType, splitTypes.SecondaryKeyType,
                                    splitTypes.SecondaryPayloadType, splitTypes.SecondaryDatasetType
                            });
                        arguments = new Object[] { dataset1, _vertex._task.SecondaryShuffleDescriptor, _vertex._task.Transforms[i] };

                        _vertex._outputKeyType = splitTypes.SecondaryKeyType;
                        _vertex._outputPayloadType = splitTypes.SecondaryPayloadType;
                        _vertex._outputDatasetType = splitTypes.SecondaryDatasetType;
                    }
                    else
                        throw new InvalidOperationException("Error: Unsupported transformation type");

                    if (method != null && generic != null && arguments != null)
                        transformOutput = generic.Invoke(this, arguments);
                    if (transformOutput != null)
                    {
                        if (!_vertex._cachedDatasets[_shardId].ContainsKey(dataset1Id))
                            _vertex._cachedDatasets[_shardId].Add(dataset1Id, transformOutput);
                        else
                            _vertex._cachedDatasets[_shardId][dataset1Id] = transformOutput;
                    }

                    _vertex._outputId = dataset1Id;
                }
            }
        }
    }
}
