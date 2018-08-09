using System;
using System.IO;
using System.Linq.Expressions;

namespace CRA.ClientLibrary.DataProcessing
{
    public interface IDataset<TKey, TPayload>
    {
        Expression<Func<IDataset<TKey, TPayload>, IDataset<TKey, TPayload>[]>> CreateSplitter(IMoveDescriptor descriptor);

        Expression<Func<IDataset<TKey, TPayload>[], IDataset<TKey, TPayload>>> CreateMerger(IMoveDescriptor descriptor);

        IDataset<TKeyNew, TPayload> ReKey<TKeyNew>(Expression<Func<TPayload, TKeyNew>> selector);

        void Subscribe(object observer);

        void Consume(object consumer);

        void ToStream(Stream stream);

        IDataset<TKey, TPayload> ToObject();

        Expression<Func<Stream, IDataset<TKey, TPayload>>> CreateFromStreamDeserializer();
    }
}
