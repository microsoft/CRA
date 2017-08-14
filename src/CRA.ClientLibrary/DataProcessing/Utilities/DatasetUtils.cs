using System;
using System.IO;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;

namespace CRA.ClientLibrary.DataProcessing
{
    [Serializable, DataContract]
    public class StructTuple<T1, T2, T3, T4>
    {
        [DataMember]
        public T1 Item1;

        [DataMember]
        public T2 Item2;

        [DataMember]
        public T3 Item3;

        [DataMember]
        public T4 Item4;

        public StructTuple() { }
    }

    [Serializable, DataContract]
    public struct Empty : IEquatable<Empty>
    {
        /// <summary>
        /// Determines whether the argument Empty value is equal to the receiver. Because Empty has a single value, this always returns true.
        /// </summary>
        /// <param name="other">An Empty value to compare to the current Empty value.</param>
        /// <returns>Because there is only one value of type Empty, this always returns true.</returns>
        public bool Equals(Empty other)
        {
            return true;
        }

        /// <summary>
        /// Determines whether the specified System.Object is equal to the current Empty.
        /// </summary>
        /// <param name="obj">The System.Object to compare with the current Empty.</param>
        /// <returns>true if the specified System.Object is a Empty value; otherwise, false.</returns>
        public override bool Equals(object obj)
        {
            return obj is Empty;
        }

        /// <summary>
        /// Returns the hash code for the Empty value.
        /// </summary>
        /// <returns>A hash code for the Empty value.</returns>
        public override int GetHashCode()
        {
            return 0;
        }

        /// <summary>
        /// Returns a string representation of the Empty value.
        /// </summary>
        /// <returns>String representation of the Empty value.</returns>
        public override string ToString()
        {
            return "()";
        }

        /// <summary>
        /// Useful method to do nothing
        /// </summary>
        [MethodImplAttribute(MethodImplOptions.AggressiveInlining)]
        public static void DoNothing()
        {
        }

        /// <summary>
        /// Determines whether the two specified Emtpy values are equal. Because Empty has a single value, this always returns true.
        /// </summary>
        /// <param name="first">The first Empty value to compare.</param>
        /// <param name="second">The second Empty value to compare.</param>
        /// <returns>Because Empty has a single value, this always returns true.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Usage", "CA1801:ReviewUnusedParameters", MessageId = "first", Justification = "Parameter required for operator overloading."), System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Usage", "CA1801:ReviewUnusedParameters", MessageId = "second", Justification = "Parameter required for operator overloading.")]
        public static bool operator ==(Empty first, Empty second)
        {
            return true;
        }

        /// <summary>
        /// Determines whether the two specified Empty values are not equal. Because Empty has a single value, this always returns false.
        /// </summary>
        /// <param name="first">The first Empty value to compare.</param>
        /// <param name="second">The second Empty value to compare.</param>
        /// <returns>Because Empty has a single value, this always returns false.</returns>
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Usage", "CA1801:ReviewUnusedParameters", MessageId = "first", Justification = "Parameter required for operator overloading."), System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Usage", "CA1801:ReviewUnusedParameters", MessageId = "second", Justification = "Parameter required for operator overloading.")]
        public static bool operator !=(Empty first, Empty second)
        {
            return false;
        }

        static readonly Empty _default = new Empty();

        /// <summary>
        /// The single Empty value.
        /// </summary>
        public static Empty Default { get { return _default; } }
    }

    public class EmptyDataSet : IDataset<Empty, Empty>
    {
        public Expression<Func<IDataset<Empty, Empty>, IDataset<Empty, Empty>[]>> CreateSplitter(IMoveDescriptor moveDescriptor)
        {
            return null;
        }

        public Expression<Func<IDataset<Empty, Empty>[], IDataset<Empty, Empty>>> CreateMerger(IMoveDescriptor moveDescriptor)
        {
            return null;
        }

        public IDataset<TKeyNew, Empty> ReKey<TKeyNew>(Expression<Func<Empty, TKeyNew>> selector)
        {
            return null;
        }

        public void ToStream(Stream stream)
        {
        }

        public Expression<Func<Stream, IDataset<Empty, Empty>>> CreateFromStreamDeserializer()
        {
            return null;
        }

        public void Subscribe(object observer){}

        public IDataset<Empty, Empty> ToObject()
        {
            throw new NotImplementedException();
        }
    }

    public class ShardedDatasetClient
    {
        public ShardedDatasetClient(){ }

        public IShardedDataset<TKey, TPayload, TDataset> CreateShardedDataset<TKey, TPayload, TDataset>(Expression<Func<int, TDataset>> sharder)
                where TDataset : IDataset<TKey, TPayload>
        {
            return new ClientSideShardedDataset<TKey, TPayload, TDataset>(sharder);
        }

    }
}
