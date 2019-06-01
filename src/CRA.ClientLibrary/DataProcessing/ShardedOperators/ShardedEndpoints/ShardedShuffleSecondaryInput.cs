using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CRA.ClientLibrary.DataProcessing
{
    public class ShardedShuffleSecondaryInput : ShardedOperatorInputBase
    {
        private ShardedShuffleOperator _vertex;

        public ShardedShuffleSecondaryInput(IVertex vertex, int shardId, int numOtherOperatorShards, string endpointName) : base(shardId, numOtherOperatorShards, endpointName)
        {
            _vertex = (ShardedShuffleOperator)vertex;
        }

        public override async Task OperatorInputFromStreamAsync(Stream stream, string otherVertex, int otherShardId, string otherEndpoint, CancellationToken token)
        {
            _startReceivingFromOtherOperatorShards.Signal();
            _startReceivingFromOtherOperatorShards.Wait();

            if (_shardId == otherShardId)
            {
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

                    _vertex._startCreatingSecondaryDatasets[otherVertex].Wait();

                    object dataset = CreateDatasetFromStream(stream, _vertex._binaryOperatorTypes[otherVertex].SecondaryKeyType,
                                    _vertex._binaryOperatorTypes[otherVertex].SecondaryPayloadType, _vertex._binaryOperatorTypes[otherVertex].SecondaryDatasetType);

                    if (!_vertex._cachedDatasets[_shardId].ContainsKey(otherVertex))
                            _vertex._cachedDatasets[_shardId].Add(otherVertex, dataset);
                    else
                            _vertex._cachedDatasets[_shardId][otherVertex] = dataset;

                    _vertex._finishCreatingSecondaryDatasets[otherVertex].Signal();

                    _vertex._runShuffleOutput.Signal();
                }

                _finishReceivingFromOtherOperatorShards.Signal();
                _finishReceivingFromOtherOperatorShards.Wait();
            }
        }
    }
}