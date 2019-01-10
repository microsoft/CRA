using CRA.ClientLibrary.DataProvider;
using System;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace CRA.ClientLibrary.DataProcessing
{
    public abstract class ShardedDatasetBase<TKey, TPayload, TDataset> :
        IShardedDataset<TKey, TPayload, TDataset>
            where TDataset : IDataset<TKey, TPayload>
    {
        private readonly IDataProvider _dataProvider;

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

        public abstract Task Subscribe<TDatasetObserver>(Expression<Func<TDatasetObserver>> observer);

        public abstract Task MultiSubscribe<TDatasetObserver>(Expression<Func<TDatasetObserver>> observer, int runsCount);

        public abstract Task Consume<TDatasetConsumer>(Expression<Func<TDatasetConsumer>> observer);
    }
}
