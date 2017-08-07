using System;
using System.Linq.Expressions;

namespace CRA.ClientLibrary.DataProcessing
{
    public interface IShardedDataset<TKey, TPayload, TDataset>
         where TDataset : IDataset<TKey, TPayload>
    {

        IShardedDataset<TKey2, TPayload2, TDataset2> Transform<TKey2, TPayload2, TDataset2>(
                Expression<Func<TDataset, TDataset2>> transformer)
            where TDataset2 : IDataset<TKey2, TPayload2>;

        IShardedDataset<TKeyO, TPayloadO, TDatasetO> Transform<TKey2, TPayload2, TDataset2,TKeyO, TPayloadO, TDatasetO>(
                IShardedDataset<TKey2, TPayload2, TDataset2> input2,
                Expression<Func<TDataset, TDataset2, TDatasetO>> transformer)
            where TDataset2 : IDataset<TKey2, TPayload2>
            where TDatasetO : IDataset<TKeyO, TPayloadO>;

        IShardedDataset<TKeyO, TPayloadO, TDatasetO> Move<TKey2, TPayload2, TDataset2, TKeyO, TPayloadO, TDatasetO>(
                Expression<Func<TDataset, IMoveDescriptor, TDataset2[]>> splitter,
                Expression<Func<TDataset2[], IMoveDescriptor, TDatasetO>> merger, IMoveDescriptor moveDescriptor)
            where TDataset2 : IDataset<TKey2, TPayload2>
            where TDatasetO : IDataset<TKeyO, TPayloadO>;

        IShardedDataset<TKey2, TPayload, TDataset2> ReKey<TKey2, TDataset2>(Expression<Func<TPayload, TKey2>> selector)
            where TDataset2 : IDataset<TKey2, TPayload>;

        IShardedDataset<TKey, TPayload, TDataset> Deploy();

        void Subscribe<TDatasetObserver>(Expression<Func<TDatasetObserver>> observer);

        void SubscribeToClient<TDatasetObserver>(Expression<Func<TDatasetObserver>> observer);
    }
}
