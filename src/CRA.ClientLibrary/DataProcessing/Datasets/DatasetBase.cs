using System;
using System.IO;
using System.Linq.Expressions;

namespace CRA.ClientLibrary.DataProcessing
{
    public class DatasetBase<TKey, TPayload> : VertexBase, IDataset<TKey, TPayload>
    {

        public DatasetBase() { }

        public Expression<Func<IDataset<TKey, TPayload>, IDataset<TKey, TPayload>[]>> CreateSplitter(IMoveDescriptor descriptor)
        {
            throw new NotImplementedException();
        }

        public Expression<Func<IDataset<TKey, TPayload>[], IDataset<TKey, TPayload>>> CreateMerger(IMoveDescriptor descriptor)
        {
            throw new NotImplementedException();
        }

        public virtual IDataset<TKeyNew, TPayload> ReKey<TKeyNew>(Expression<Func<TPayload, TKeyNew>> selector)
        {
            throw new NotImplementedException();
        }

        public virtual void ToStream(Stream stream)
        {
            throw new NotImplementedException();
        }

        public virtual Expression<Func<Stream, IDataset<TKey, TPayload>>> CreateFromStreamDeserializer()
        {
            throw new NotImplementedException();
        }

        public virtual void Subscribe(object observer)
        {
            throw new NotImplementedException();
        }

        public virtual void Consume(object consumer)
        {
            throw new NotImplementedException();
        }


        public virtual IDataset<TKey, TPayload> ToObject()
        {
            throw new NotImplementedException();
        }
    }
}
