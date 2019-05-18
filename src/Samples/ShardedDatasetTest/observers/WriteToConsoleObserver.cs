using System;
using System.Collections.Generic;
using System.Text;

namespace ShardedDatasetTest
{
    public class WriteToConsoleObserver<TKey, TPayload> : IIntKeyedDatasetObserver<TKey, TPayload>
    {
        public WriteToConsoleObserver()
        {
        }

        public void ProcessIntKeyedDataset(IntKeyedDataset<TKey, TPayload> source)
        {
            Console.WriteLine("Shift value is: "  + source._shift);
        }
    }
}
