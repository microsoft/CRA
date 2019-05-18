using System;
using System.IO;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CRA.ClientLibrary.DataProcessing
{
    public class ShardedSubscribeClientOutput : ShardedOperatorOutputBase
    {
        private ShardedSubscribeClientOperator _vertex;

        public ShardedSubscribeClientOutput(IVertex vertex, int shardId, int numOtherOperatorShards, string endpointName) : base(shardId, numOtherOperatorShards, endpointName)
        {
            _vertex = (ShardedSubscribeClientOperator)vertex;
        }

        public override async Task OperatorOutputToStreamAsync(Stream stream, string otherVertex, int otherShardId, string otherEndpoint, CancellationToken token)
        {
            MemoryStream memStream = (MemoryStream)stream;
            memStream.Seek(0, SeekOrigin.Begin);

            // Start deploying
            memStream.Read(_deployMsgBuffer, 0, _deployMsgBuffer.Length);
            if (Encoding.ASCII.GetString(_deployMsgBuffer).Equals("DEPLOY"))
            {
                _vertex._deploySubscribeInput.Signal();
                _vertex._deploySubscribeOutput.Wait();

                // Start running
                memStream.Read(_runMsgBuffer, 0, _runMsgBuffer.Length);
                if (Encoding.ASCII.GetString(_runMsgBuffer).Equals("RUN"))
                {
                    _vertex._outputObserver = Encoding.UTF8.GetString(memStream.ReadByteArray());

                    _vertex._runSubscribeInput.Signal();
                    _vertex._runSubscribeOutput.Wait();

                    ApplySubscribe();
                }
            }
        }

        private void ApplySubscribe()
        {
            MethodInfo method = typeof(ShardedOperatorOutputBase).GetMethod("SubscribeObserver"); 
            MethodInfo generic = method.MakeGenericMethod(
                new Type[] {_vertex._task.OperationTypes.OutputKeyType,
                            _vertex._task.OperationTypes.OutputPayloadType,
                            _vertex._task.OperationTypes.OutputDatasetType});

            foreach (var ind in _vertex._cachedDatasets[_shardId].Keys)
            {
                try
                {
                    Task.Run(() => generic.Invoke(this, new Object[] { _vertex._cachedDatasets[_shardId][ind], _vertex._outputObserver}));
                }
                catch (Exception e)
                {
                    Console.WriteLine("Error: CRA failed to apply the subscribe() in the client side. " + e.ToString());
                }
            }
        }
    }
}