using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CRA.ClientLibrary.DataProcessing
{
    public abstract class ShardedOperatorInputBase : AsyncShardedVertexInputEndpointBase
    {
        protected int _shardId;
        protected string _endpointName;

        protected byte[] _deployMsgBuffer;
        protected byte[] _runMsgBuffer;

        protected CountdownEvent _startReceivingFromOtherOperatorShards;
        protected CountdownEvent _finishReceivingFromOtherOperatorShards;

        protected Dictionary<int, bool> _isEndpointEstablished;

        public ShardedOperatorInputBase(int shardId, int numOtherOperatorShards, string endpointName)
        {
            _shardId = shardId;
            _endpointName = endpointName;

            _deployMsgBuffer = new byte[Encoding.ASCII.GetBytes("DEPLOY").Length];
            _deployMsgBuffer = Encoding.ASCII.GetBytes("DEPLOY");

            _runMsgBuffer = new byte[Encoding.ASCII.GetBytes("RUN").Length];
            _runMsgBuffer = Encoding.ASCII.GetBytes("RUN");

            _startReceivingFromOtherOperatorShards = new CountdownEvent(numOtherOperatorShards);
            _finishReceivingFromOtherOperatorShards = new CountdownEvent(numOtherOperatorShards);

            _isEndpointEstablished = new Dictionary<int, bool>();
        }

        public override void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                Console.WriteLine("Disposing " + GetType().Name);
            }
        }

        public override async Task FromStreamAsync(Stream stream, string otherVertex, int otherShardId, string otherEndpoint, CancellationToken token)
        {
            if (!(_isEndpointEstablished.ContainsKey(otherShardId) && _isEndpointEstablished[otherShardId]))
            {
                _isEndpointEstablished[otherShardId] = true;
                await OperatorInputFromStreamAsync(stream, otherVertex, otherShardId, otherEndpoint, token);
            }
        }

     
    public abstract Task OperatorInputFromStreamAsync(Stream stream, string otherVertex, int otherShardId, string otherEndpoint, CancellationToken token);

        public object CreateDatasetFromStream(Stream stream, Type inputKeyType, Type inputPayloadType, Type inputDatasetType)
        {
            try
            {
                MethodInfo method = typeof(ShardedOperatorInputBase).GetMethod("CreateDataset");
                MethodInfo generic = method.MakeGenericMethod(
                                        new Type[] { inputKeyType, inputPayloadType, inputDatasetType });
                return generic.Invoke(this, new Object[] { stream });
            }
            catch (Exception e)
            {
                throw new Exception("Error: Failed to create dataset from Stream!! " + e.ToString());
            }
        }

        public object CreateDataset<TKey, TPayload, TDataset>(Stream stream)
            where TDataset : IDataset<TKey, TPayload>
        {
            TDataset templateDataset = (TDataset)Activator.CreateInstance(typeof(TDataset));
            var compiledCreator = templateDataset.CreateFromStreamDeserializer().Compile();
            return (TDataset)compiledCreator(stream);
        }       
    }
}
