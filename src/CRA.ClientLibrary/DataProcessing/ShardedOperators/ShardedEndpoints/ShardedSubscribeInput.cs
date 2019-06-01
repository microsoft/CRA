using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CRA.ClientLibrary.DataProcessing
{
    public class ShardedSubscribeInput : ShardedOperatorInputBase
    {
        private ShardedSubscribeOperator _vertex;

        public ShardedSubscribeInput(IVertex vertex, int shardId, int numOtherOperatorShards, string endpointName) : base(shardId, numOtherOperatorShards, endpointName)
        {
            _vertex = (ShardedSubscribeOperator)vertex;
        }

        public override async Task OperatorInputFromStreamAsync(Stream stream, string otherVertex, int otherShardId, string otherEndpoint, CancellationToken token)
        {
            _startReceivingFromOtherOperatorShards.Signal();
            _startReceivingFromOtherOperatorShards.Wait();

            if (_shardId == otherShardId)
            {
                // Start deploying
                _vertex._deploySubscribeInput.Wait();

                await stream.WriteAsync(_deployMsgBuffer, 0, _deployMsgBuffer.Length);
                await stream.ReadAllRequiredBytesAsync(_deployMsgBuffer, 0, _deployMsgBuffer.Length);
                if (Encoding.ASCII.GetString(_deployMsgBuffer).Equals("DEPLOY"))
                {
                    _vertex._deploySubscribeOutput.Signal();

                    // Start running
                    _vertex._runSubscribeInput.Wait();

                    await stream.WriteAsync(_runMsgBuffer, 0, _runMsgBuffer.Length);

                    if (!_vertex._cachedDatasets[_shardId].ContainsKey(_vertex._task.InputIds.InputId1))
                    {
                        object dataset = CreateDatasetFromStream(stream, _vertex._task.OperationTypes.OutputKeyType,
                                               _vertex._task.OperationTypes.OutputPayloadType, _vertex._task.OperationTypes.OutputDatasetType);
                        _vertex._cachedDatasets[_shardId].Add(_vertex._task.InputIds.InputId1, dataset);
                    }

                    _vertex._runSubscribeOutput.Signal();
                }
            }

            _finishReceivingFromOtherOperatorShards.Signal();
            _finishReceivingFromOtherOperatorShards.Wait();
        }
    }
}
