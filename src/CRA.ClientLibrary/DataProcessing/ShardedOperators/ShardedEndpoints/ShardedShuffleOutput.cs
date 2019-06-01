using System;
using System.IO;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CRA.ClientLibrary.DataProcessing
{
    public class ShardedShuffleOutput : ShardedOperatorOutputBase
    {
        private ShardedShuffleOperator _vertex;

        public ShardedShuffleOutput(IVertex vertex, int shardId, int numOtherOperatorShards, string endpointName) : base(shardId, numOtherOperatorShards, endpointName)
        {
            _vertex = (ShardedShuffleOperator)vertex;
        }

        public override async Task OperatorOutputToStreamAsync(Stream stream, string otherVertex, int otherShardId, string otherEndpoint, CancellationToken token)
        {
            _startSendingToOtherOperatorShards.Signal();
            _startSendingToOtherOperatorShards.Wait();

            if (!_vertex._hasSplittedOutput)
            {
                if (_shardId == otherShardId)
                {
                    // Start deploying
                    await stream.ReadAllRequiredBytesAsync(_deployMsgBuffer, 0, _deployMsgBuffer.Length);
                    if (Encoding.ASCII.GetString(_deployMsgBuffer).Equals("DEPLOY"))
                    {
                        _vertex._deployShuffleInput.Signal();
                        _vertex._deployShuffleOutput.Wait();

                        await stream.WriteAsync(_deployMsgBuffer, 0, _deployMsgBuffer.Length);

                        // Start running
                        await stream.ReadAllRequiredBytesAsync(_runMsgBuffer, 0, _runMsgBuffer.Length);
                        if (Encoding.ASCII.GetString(_runMsgBuffer).Equals("RUN"))
                        {
                            _vertex._runShuffleInput.Signal();
                            _vertex._runShuffleOutput.Wait();

                            MethodInfo method = typeof(ShardedOperatorOutputBase).GetMethod("StartProducer");
                            MethodInfo generic = method.MakeGenericMethod(
                                    new Type[] { _vertex._outputKeyType, _vertex._outputPayloadType, _vertex._outputDatasetType });
                            generic.Invoke(this, new Object[] { _vertex._cachedDatasets[_shardId][_vertex._outputId], stream });
                        }
                    }
                }
            }
            else
            {
                // Start deploying
                await stream.ReadAllRequiredBytesAsync(_deployMsgBuffer, 0, _deployMsgBuffer.Length);
                if (Encoding.ASCII.GetString(_deployMsgBuffer).Equals("DEPLOY"))
                {
                    _vertex._deployShuffleInput.Signal();
                    _vertex._deployShuffleOutput.Wait();

                    await stream.WriteAsync(_deployMsgBuffer, 0, _deployMsgBuffer.Length);

                    // Start running
                    await stream.ReadAllRequiredBytesAsync(_runMsgBuffer, 0, _runMsgBuffer.Length);
                    if (Encoding.ASCII.GetString(_runMsgBuffer).Equals("RUN"))
                    {
                        _vertex._runShuffleInput.Signal();
                        _vertex._runShuffleOutput.Wait();

                        object[] splitDatasets = (object[])_vertex._cachedDatasets[_shardId][_vertex._outputId];
                        MethodInfo method = typeof(ShardedOperatorOutputBase).GetMethod("StartProducer");
                        MethodInfo generic = method.MakeGenericMethod(
                                new Type[] { _vertex._outputKeyType, _vertex._outputPayloadType, _vertex._outputDatasetType });
                        generic.Invoke(this, new Object[] { splitDatasets[otherShardId], stream });
                    }
                }
            }

            _finishSendingToOtherOperatorShards.Signal();
            _finishSendingToOtherOperatorShards.Wait();
        }
    }
}
