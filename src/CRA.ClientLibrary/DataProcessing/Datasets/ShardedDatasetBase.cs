using System;
using System.Linq.Expressions;

namespace CRA.ClientLibrary.DataProcessing
{
    public abstract class ShardedDatasetBase<TKey, TPayload, TDataset> : IShardedDataset<TKey, TPayload, TDataset>
            where TDataset : IDataset<TKey, TPayload>
    {
        public IShardedDataset<TKey2, TPayload2, TDataset2> Transform<TKey2, TPayload2, TDataset2>(
               Expression<Func<TDataset, TDataset2>> transformer)
           where TDataset2 : IDataset<TKey2, TPayload2>
        {
            transformer = new ClosureEliminator().Visit(transformer) as Expression<Func<TDataset, TDataset2>>;

            return new DeployableShardedDataset<TKey, TPayload, TDataset,
                        Empty, Empty, EmptyDataSet,TKey2, TPayload2, TDataset2>(this, transformer);
        }

        public IShardedDataset<TKeyO, TPayloadO, TDatasetO> Transform<TKey2, TPayload2, TDataset2, TKeyO, TPayloadO, TDatasetO>(
                IShardedDataset<TKey2, TPayload2, TDataset2> input2,
                Expression<Func<TDataset, TDataset2, TDatasetO>> transformer)
            where TDataset2 : IDataset<TKey2, TPayload2>
            where TDatasetO : IDataset<TKeyO, TPayloadO>
        {
            transformer = new ClosureEliminator().Visit(transformer) as Expression<Func<TDataset, TDataset2, TDatasetO>>;

            return new DeployableShardedDataset<TKey, TPayload, TDataset,
                        TKey2, TPayload2, TDataset2, TKeyO, TPayloadO, TDatasetO>(this, input2, transformer);
        }


        public IShardedDataset<TKeyO, TPayloadO, TDatasetO> Move<TKey2, TPayload2, TDataset2, TKeyO, TPayloadO, TDatasetO>(
                Expression<Func<TDataset, IMoveDescriptor, TDataset2[]>> mapper, 
                Expression<Func<TDataset2[], IMoveDescriptor, TDatasetO>> reducer, IMoveDescriptor moveDescriptor)
            where TDataset2 : IDataset<TKey2, TPayload2>
            where TDatasetO : IDataset<TKeyO, TPayloadO>
        {
            return new DeployableShardedDataset<TKey, TPayload, TDataset, TKey2, TPayload2, TDataset2, TKeyO, TPayloadO, TDatasetO>(
                        this, mapper, reducer, moveDescriptor);
        }

        public IShardedDataset<TKey2, TPayload, TDataset2> ReKey<TKey2, TDataset2>(
                Expression<Func<TPayload, TKey2>> selector) 
            where TDataset2 : IDataset<TKey2, TPayload>
        {
            return Transform<TKey2, TPayload, TDataset2>(ds => (TDataset2)ds.ReKey(selector));
        }

        public abstract IShardedDataset<TKey, TPayload, TDataset> Deploy();

        public abstract void Subscribe<TDatasetObserver>(Expression<Func<TDatasetObserver>> observer);

        public abstract void SubscribeToClient<TDatasetObserver>(Expression<Func<TDatasetObserver>> observer);
    }
}
