using System;
using System.IO;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CRA.ClientLibrary.DataProcessing
{
    public class ShardedSubscribeOutput : ShardedOperatorOutputBase
    {
        private ShardedSubscribeOperator _vertex;

        public ShardedSubscribeOutput(IVertex vertex, int shardId, int numOtherOperatorShards, string endpointName) : base(shardId, numOtherOperatorShards, endpointName)
        {
            _vertex = (ShardedSubscribeOperator)vertex;
        }

        public override async Task OperatorOutputToStreamAsync(Stream stream, string otherVertex, int otherShardId, string otherEndpoint, CancellationToken token)
        {
            _sendToOtherOperatorShards.Signal();
            _sendToOtherOperatorShards.Wait();

            if (_shardId == otherShardId)
            {
                // Start deploying
                await stream.ReadAllRequiredBytesAsync(_deployMsgBuffer, 0, _deployMsgBuffer.Length);
                if (Encoding.ASCII.GetString(_deployMsgBuffer).Equals("DEPLOY"))
                {
                    _vertex._deploySubscribeInput.Signal();
                    _vertex._deploySubscribeOutput.Wait();

                    await stream.WriteAsync(_deployMsgBuffer, 0, _deployMsgBuffer.Length);

                    // Start running
                    await stream.ReadAllRequiredBytesAsync(_runMsgBuffer, 0, _runMsgBuffer.Length);
                    if (Encoding.ASCII.GetString(_runMsgBuffer).Equals("RUN"))
                    {
                        _vertex._outputObserver = Encoding.UTF8.GetString(stream.ReadByteArray());

                        _vertex._runSubscribeInput.Signal();
                        _vertex._runSubscribeOutput.Wait();

                        ApplySubscribe();
                    }
                }
            }
        }

        private void ApplySubscribe()
        {
            MethodInfo method = typeof(ShardedSubscribeOperator).GetMethod("SubscribeObserver");
            MethodInfo generic = method.MakeGenericMethod(
                    new Type[] {_vertex._task.OperationTypes.OutputKeyType,
                                _vertex._task.OperationTypes.OutputPayloadType,
                                _vertex._task.OperationTypes.OutputDatasetType});
            generic.Invoke(this, new Object[] { _vertex._cachedDatasets[_shardId][_vertex._task.InputIds.InputId1], _vertex._outputObserver});
        }
    }
}
