using CRA.ClientLibrary.DataProvider;
using System;
using System.IO;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CRA.ClientLibrary.DataProcessing
{
    public abstract class ShardedDatasetBase<TKey, TPayload, TDataset> :
        IShardedDataset<TKey, TPayload, TDataset>
            where TDataset : IDataset<TKey, TPayload>
    {
        private readonly IDataProvider _dataProvider;

        protected bool _isDeployed = false;

        protected CRAWorker _craWorker = null;
        protected ClientTerminalTask _clientTerminalTask;

        public ShardedDatasetBase(IDataProvider dataProvider)
        {
            _dataProvider = dataProvider;
        }

        public IShardedDataset<TKey2, TPayload2, TDataset2> Transform<TKey2, TPayload2, TDataset2>(
            Expression<Func<TDataset, TDataset2>> transformer)
            where TDataset2 : IDataset<TKey2, TPayload2>
        {
            transformer = new ClosureEliminator().Visit(transformer) as Expression<Func<TDataset, TDataset2>>;

            return new DeployableShardedDataset<TKey, TPayload, TDataset, Empty, Empty, EmptyDataSet,TKey2, TPayload2, TDataset2>(
                _dataProvider,
                this,
                transformer);
        }

        public IShardedDataset<TKeyO, TPayloadO, TDatasetO> Transform<TKey2, TPayload2, TDataset2, TKeyO, TPayloadO, TDatasetO>(
                IShardedDataset<TKey2, TPayload2, TDataset2> input2,
                Expression<Func<TDataset, TDataset2, TDatasetO>> transformer)
            where TDataset2 : IDataset<TKey2, TPayload2>
            where TDatasetO : IDataset<TKeyO, TPayloadO>
        {
            transformer = new ClosureEliminator().Visit(transformer) as Expression<Func<TDataset, TDataset2, TDatasetO>>;

            return new DeployableShardedDataset<TKey, TPayload, TDataset, TKey2, TPayload2, TDataset2, TKeyO, TPayloadO, TDatasetO>(
                _dataProvider,
                this,
                input2,
                transformer);
        }


        public IShardedDataset<TKeyO, TPayloadO, TDatasetO> Move<TKey2, TPayload2, TDataset2, TKeyO, TPayloadO, TDatasetO>(
                Expression<Func<TDataset, IMoveDescriptor, TDataset2[]>> mapper, 
                Expression<Func<TDataset2[], IMoveDescriptor, TDatasetO>> reducer, IMoveDescriptor moveDescriptor)
            where TDataset2 : IDataset<TKey2, TPayload2>
            where TDatasetO : IDataset<TKeyO, TPayloadO>
        {
            return new DeployableShardedDataset<TKey, TPayload, TDataset, TKey2, TPayload2, TDataset2, TKeyO, TPayloadO, TDatasetO>(
                _dataProvider,
                this,
                mapper,
                reducer,
                moveDescriptor);
        }

        public IShardedDataset<TKey2, TPayload, TDataset2> ReKey<TKey2, TDataset2>(
                Expression<Func<TPayload, TKey2>> selector) 
            where TDataset2 : IDataset<TKey2, TPayload>
        {
            return Transform<TKey2, TPayload, TDataset2>(ds => (TDataset2)ds.ReKey(selector));
        }

        public abstract Task<IShardedDataset<TKey, TPayload, TDataset>> Deploy();

        public  async Task Subscribe<TDatasetObserver>(Expression<Func<TDatasetObserver>> observer)
        {
            object deployedDataset = null;
            if (!_isDeployed)
            {
                deployedDataset = await Deploy();
            }

            if (!_isDeployed && deployedDataset == null)
                throw new InvalidOperationException();
            else
            {
                var deployMsgBuffer = new byte[Encoding.ASCII.GetBytes("DEPLOY").Length];
                deployMsgBuffer = Encoding.ASCII.GetBytes("DEPLOY");

                var runMsgBuffer = new byte[Encoding.ASCII.GetBytes("RUN").Length];
                runMsgBuffer = Encoding.ASCII.GetBytes("RUN");

                MemoryStream stream = new MemoryStream();

                CancellationTokenSource source = new CancellationTokenSource();

                foreach (var key in _craWorker._localVertexTable[_clientTerminalTask.OutputId + "$0"].AsyncOutputEndpoints.Keys)
                {
                    stream.Write(deployMsgBuffer, 0, deployMsgBuffer.Length);
                    stream.Write(runMsgBuffer, 0, runMsgBuffer.Length);
                    stream.WriteByteArray(Encoding.UTF8.GetBytes(SerializationHelper.Serialize(observer)));

#pragma warning disable CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed
                    Task.Run(() => ((IAsyncShardedVertexOutputEndpoint)_craWorker._localVertexTable[_clientTerminalTask.OutputId + "$0"].AsyncOutputEndpoints[key])
                            .ToStreamAsync(stream, "", 0, "", source.Token));
#pragma warning restore CS4014 // Because this call is not awaited, execution of the current method continues before the call is completed

                    break;
                }
            }
        }

        public async Task MultiSubscribe<TDatasetObserver>(Expression<Func<TDatasetObserver>> observer, int runsCount)
        {
            throw new NotImplementedException();
        }
    }
}
