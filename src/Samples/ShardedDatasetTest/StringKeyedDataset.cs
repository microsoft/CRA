using CRA.ClientLibrary;
using CRA.ClientLibrary.DataProcessing;
using System;
using System.IO;
using System.Linq.Expressions;

namespace ShardedDatasetTest
{
    public class StringKeyedDataset<TKey, TPayload> : DatasetBase<TKey, TPayload>
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

        protected string[] _keys;
        protected int[] _values;
        public int _shift;

        public StringKeyedDataset()
        {
        }

        public StringKeyedDataset(int shift)
        {
            _shift = shift;
            _keys = new string[20];
            _values = new int[20];
            for (int i = 0; i < _values.Length; i++)
            {
                _keys[i] = (i + shift) + "";
                _values[i] = i + shift;
            }
        }

        public StringKeyedDataset(StringKeyedDataset<TKey, TPayload> intDataset) : this(intDataset._shift)
        {
            _shardingCardinality = intDataset.ShardingCardinality;
        }

        public override IDataset<TKeyNew, TPayload> ReKey<TKeyNew>(Expression<Func<TPayload, TKeyNew>> selector)
        {
            return null;
        }

        public override void ToStream(Stream stream)
        {
            byte[] intBytes = BitConverter.GetBytes(_shift);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(intBytes);
            byte[] result = intBytes;

            stream.WriteAsync(result, 0, result.Length);
        }


        private IDataset<TKey, TPayload> CreateStreamableDatasetFromStream(Stream stream)
        {
            byte[] intBytes = new byte[BitConverter.GetBytes(_shift).Length];
            stream.ReadAllRequiredBytes(intBytes, 0, intBytes.Length);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(intBytes);
            _shift = BitConverter.ToInt32(intBytes, 0);

            return (IDataset<TKey, TPayload>)new StringKeyedDataset<string, int>(_shift);
        }


        public override Expression<Func<Stream, IDataset<TKey, TPayload>>> CreateFromStreamDeserializer()
        {
            return stream => (new StringKeyedDataset<TKey, TPayload>()).CreateStreamableDatasetFromStream(stream);
        }

        public override void Subscribe(object observer)
        {
            throw new NotImplementedException();
        }
    }
}
