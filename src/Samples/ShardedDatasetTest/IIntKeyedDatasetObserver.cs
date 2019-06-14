using System;
using System.Collections.Generic;
using System.Text;

namespace ShardedDatasetTest
{
    public interface IIntKeyedDatasetObserver<TKey, TPayload>
    {
         void ProcessIntKeyedDataset(IntKeyedDataset<TKey, TPayload> source);
    }
}
