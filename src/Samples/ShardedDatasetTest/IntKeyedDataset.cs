using CRA.ClientLibrary;
using CRA.ClientLibrary.DataProcessing;
using System;
using System.IO;
using System.Linq.Expressions;

namespace ShardedDatasetTest
{
    public class IntKeyedDataset<TKey, TPayload> : DatasetBase<TKey, TPayload>
    {
        protected int _shardingCardinality;

        public int ShardingCardinality
        {
            get
            {
                return _shardingCardinality;
            }

            set
            {
                _shardingCardinality = value;
            }
        }

        protected int[] _keys;
        protected int[] _values;
        public int _shift;

        public IntKeyedDataset()
        {
        }

        public IntKeyedDataset(int shift)
        {
            _shift = shift;
            _keys = new int[20];
            _values = new int[20];
            for (int i = 0; i < _values.Length; i++)
            {
                _keys[i] = i + shift;
                _values[i] = i + shift;
            }
        }

        public IntKeyedDataset(IntKeyedDataset<TKey, TPayload> intDataset) : this(intDataset._shift)
        {
            _shardingCardinality = intDataset.ShardingCardinality;
            _shift = intDataset._shift;
            _keys = intDataset._keys;
            _values = intDataset._values;
        }

        public override IDataset<TKeyNew, TPayload> ReKey<TKeyNew>(Expression<Func<TPayload, TKeyNew>> selector)
        {
            if (selector is Expression<Func<int, string>>)
            {
                return (IDataset<TKeyNew, TPayload>)new StringKeyedDataset<string, int>(_shift);
            }
            else
            {
                throw new InvalidCastException();
            }
        }

        public override void ToStream(Stream stream)
        {
            byte[] intBytes = BitConverter.GetBytes(_shift);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(intBytes);
            byte[] result = intBytes;

            Console.WriteLine("Current IntKeyedDataset Shift ToStream: " + _shift);
            stream.WriteAsync(result, 0, result.Length);
        }


        private IDataset<TKey, TPayload> CreateStreamableDatasetFromStream(Stream stream)
        {
            byte[] intBytes = new byte[BitConverter.GetBytes(_shift).Length];
            stream.ReadAllRequiredBytes(intBytes, 0, intBytes.Length);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(intBytes);
            _shift = BitConverter.ToInt32(intBytes, 0);

            Console.WriteLine("Current IntKeyedDataset Shift FromStream: " + _shift);
            return (IDataset<TKey, TPayload>)new IntKeyedDataset<int, int>(_shift);
        }


        public override Expression<Func<Stream, IDataset<TKey, TPayload>>> CreateFromStreamDeserializer()
        {
            return stream => (new IntKeyedDataset<TKey, TPayload>()).CreateStreamableDatasetFromStream(stream);
        }

        public override void Subscribe(object observer)
        {
            if (observer is IIntKeyedDatasetObserver<TKey, TPayload>)
            {
                ((IIntKeyedDatasetObserver<TKey, TPayload>)observer).ProcessIntKeyedDataset(this);
            }
            else
                throw new InvalidCastException();
        }

    }
}
