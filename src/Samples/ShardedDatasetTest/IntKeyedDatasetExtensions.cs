using CRA.ClientLibrary.DataProcessing;
using System;
using System.Collections.Generic;
using System.Text;

namespace ShardedDatasetTest
{
    public static class IntKeyedDatasetExtensions
    {
        public static IntKeyedDataset<int, int> ShiftUp(this IntKeyedDataset<int, int> source, int upAmount)
        {
            return new IntKeyedDataset<int, int>(source._shift + upAmount);
        }

        public static IntKeyedDataset<int, int> BinaryShiftUp(this IntKeyedDataset<int, int> source, IntKeyedDataset<int, int> inputDS, int upAmount)
        {
            return new IntKeyedDataset<int, int>(source._shift + inputDS._shift + upAmount);
        }


        public static IntKeyedDataset<int, int>[] Splitter(this IntKeyedDataset<int, int> source, IMoveDescriptor descriptor)
        {
            IntKeyedDataset<int, int>[] outputs = new IntKeyedDataset<int, int>[2];
            outputs[0] = new IntKeyedDataset<int, int>(source._shift + 100);
            outputs[1] = new IntKeyedDataset<int, int>(source._shift + 200);
            return outputs;
        }

        public static IntKeyedDataset<int, int> Merger(this IntKeyedDataset<int, int>[] source, IMoveDescriptor descriptor)
        {
            return new IntKeyedDataset<int, int>(source[0]._shift + source[1]._shift);
        }
    }
}
