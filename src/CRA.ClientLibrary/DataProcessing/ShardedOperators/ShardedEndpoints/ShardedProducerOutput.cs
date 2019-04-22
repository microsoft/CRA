using System;
using System.IO;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CRA.ClientLibrary.DataProcessing
{
    public class ShardedProducerOutput : ShardedOperatorOutputBase
    {
        private ShardedProducerOperator _vertex;

        public ShardedProducerOutput(IVertex vertex, int shardId, int numOtherOperatorShards, string endpointName) : base(shardId, numOtherOperatorShards, endpointName)
        {
            _vertex = (ShardedProducerOperator)vertex;
        }

        public override async Task OperatorOutputToStreamAsync(Stream stream, string otherVertex, int otherShardId, string otherEndpoint, CancellationToken token)
        {
            _sendToOtherOperatorShards.Signal();  
            _sendToOtherOperatorShards.Wait();

            //TODO: Here we are assuming that we are starting from producing directly, and no inputs exist
            //TODO: we are starting running from here, and assuming all deployment has been done before, change in case we have an input
            if (!_vertex._hasSplittedOutput)
            {
                if (_shardId == otherShardId)
                {
                    await stream.ReadAllRequiredBytesAsync(_deployMsgBuffer, 0, _deployMsgBuffer.Length);
                    if (Encoding.ASCII.GetString(_deployMsgBuffer).Equals("DEPLOY"))
                    {
                        // Start deploying
                        if (!_vertex._hasSecondaryInput)
                        {
                            await stream.WriteAsync(_deployMsgBuffer, 0, _deployMsgBuffer.Length);
                        }
                        else
                        {
                            //TODO: handle secondary input, similar to shuffle output and its relation to shuffle input
                        }

                        // Start running
                        await stream.ReadAllRequiredBytesAsync(_runMsgBuffer, 0, _runMsgBuffer.Length);
                        if (Encoding.ASCII.GetString(_runMsgBuffer).Equals("RUN"))
                        {
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
                await stream.ReadAllRequiredBytesAsync(_deployMsgBuffer, 0, _deployMsgBuffer.Length);
                if (Encoding.ASCII.GetString(_deployMsgBuffer).Equals("DEPLOY"))
                {
                    // Start deploying
                    if (!_vertex._hasSecondaryInput)
                    {
                        await stream.WriteAsync(_deployMsgBuffer, 0, _deployMsgBuffer.Length);
                    }
                    else
                    {
                        //TODO: handle secondary input, similar to shuffle output and its relation to shuffle input
                    }

                    // Start running
                    await stream.ReadAllRequiredBytesAsync(_runMsgBuffer, 0, _runMsgBuffer.Length);
                    if (Encoding.ASCII.GetString(_runMsgBuffer).Equals("RUN"))
                    {
                        object[] splitDatasets = (object[])_vertex._cachedDatasets[_shardId][_vertex._outputId];
                        MethodInfo method = typeof(ShardedOperatorOutputBase).GetMethod("StartProducer");
                        MethodInfo generic = method.MakeGenericMethod(
                                new Type[] { _vertex._outputKeyType, _vertex._outputPayloadType, _vertex._outputDatasetType });
                        generic.Invoke(this, new Object[] { splitDatasets[otherShardId], stream });
                    }
                }
            }
        }
    }
}
