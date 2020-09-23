using System;
using System.Diagnostics;
using System.Linq.Expressions;
using System.Runtime.Serialization;

namespace CRA.ClientLibrary.DataProcessing
{
    public enum MoveScope { Local, Global, GlobalAndLocal }

    public interface IMoveDescriptor
    {
        object GetLocation();

        int GetFlatValue();
    }

    public static class MoveDescriptor
    {
        public static IMoveDescriptor MoveBase(int flatValue = -1, int shardsCount = 0)
        {
            return new MoveDescriptorBase(flatValue, shardsCount);
        }

        public static IMoveDescriptor Global(int flatValue = -1, ICRACluster cluster = null)
        {
            return new GlobalDescriptor(flatValue, cluster);
        }

        public static IMoveDescriptor GlobalAndLocal(int flatValue = -1, ICRACluster cluster = null, int shardsCount = 0)
        {
            return new GlobalAndLocalDescriptor(flatValue, cluster, shardsCount);
        }
    }

    public static class MoveUtils
    {
        public static IMoveDescriptor PopulateMoveDescriptor(MoveScope moveScope, IDeployDescriptor deploymentDescriptor)
        {
            var deploymentMap = deploymentDescriptor.InstancesMap();
            int shardsCount = 0;
            foreach (var key in deploymentMap.Keys)
                shardsCount += deploymentMap[key];

            switch (moveScope)
            {
                case MoveScope.Local:
                    return MoveDescriptor.MoveBase(shardsCount, shardsCount);
                case MoveScope.Global:
                    return MoveDescriptor.MoveBase(-1, shardsCount);
                default:
                    return MoveDescriptor.MoveBase(0, shardsCount);
            }
        }

        public static object ApplySplitter<TKeyI1, TPayloadI1, TDatasetI1, TKeyI2, TPayloadI2, TDatasetI2>(
                object dataset, IMoveDescriptor moveDescriptor, string splitter)
            where TDatasetI1 : IDataset<TKeyI1, TPayloadI1>
            where TDatasetI2 : IDataset<TKeyI2, TPayloadI2>
        {
            try
            {
                if (splitter != null)
                {
                    Expression transformer = SerializationHelper.Deserialize(splitter);
                    var compiledTransformer = Expression<Func<TDatasetI1, IMoveDescriptor, TDatasetI2[]>>.Lambda(transformer).Compile();
                    Delegate compiledTransformerConstructor = (Delegate)compiledTransformer.DynamicInvoke();
                    return compiledTransformerConstructor.DynamicInvoke((TDatasetI1)dataset, moveDescriptor);
                }
            }
            catch (Exception e)
            {
                Trace.TraceError("Error: the CRA vertex failed to apply a splitter transform for an error of type " + e.GetType() + ": " + e.ToString());
            }

            return null;
        }

        public static object ApplyMerger<TKeyI2, TPayloadI2, TDatasetI2, TKeyO, TPayloadO, TDatasetO>(
                   object[] datasets, IMoveDescriptor moveDescriptor, string merger)
            where TDatasetI2 : IDataset<TKeyI2, TPayloadI2>
            where TDatasetO : IDataset<TKeyO, TPayloadO>
        {
            try
            {
                if (merger != null)
                {
                    TDatasetI2[] transientDatasets = new TDatasetI2[datasets.Length];
                    for (int i = 0; i < transientDatasets.Length; i++)
                            transientDatasets[i] = (TDatasetI2)datasets[i];

                    Expression transformer = SerializationHelper.Deserialize(merger);
                    var compiledTransformer = Expression<Func<TDatasetI2[], IMoveDescriptor, TDatasetO>>.Lambda(transformer).Compile();
                    Delegate compiledTransformerConstructor = (Delegate)compiledTransformer.DynamicInvoke();
                    return compiledTransformerConstructor.DynamicInvoke(transientDatasets, moveDescriptor);
                }
            }
            catch (Exception e)
            {
                Trace.TraceError("Error: The CRA vertex failed to apply a mergere transform for an error of type " + e.GetType() + " : " + e.ToString());
            }

            return null;
        }
    }

    [DataContract]
    public class MoveDescriptorBase : IMoveDescriptor
    {
        [DataMember]
        private int _flatValue;

        [DataMember]
        private int _shardsCount = 0;

        public MoveDescriptorBase(int flatValue, int shardsCount = 0)
        {
            _flatValue = flatValue;
            _shardsCount = shardsCount;
        }

        public int GetFlatValue()
        {
            return _flatValue;
        }

        public object GetLocation()
        {
            return _shardsCount;
        }
    }

    public interface ICRACluster { }

    [DataContract]
    public class GlobalDescriptor : IMoveDescriptor
    {
        [DataMember]
        private int _flatValue;

        [DataMember]
        private ICRACluster _cluster;

        public GlobalDescriptor(int flatValue, ICRACluster cluster)
        {
            _flatValue = flatValue;
            _cluster = cluster;
        }

        public int GetFlatValue()
        {
            return _flatValue;
        }

        public object GetLocation()
        {
            return _cluster;
        }
    }

    [DataContract]
    public class GlobalAndLocalDescriptor : IMoveDescriptor
    {
        [DataMember]
        private int _flatValue;

        [DataMember]
        private ICRACluster _cluster;

        [DataMember]
        private int _shardsCount;

        public GlobalAndLocalDescriptor(int flatValue, ICRACluster cluster = null, int shardsCount = 0)
        {
            _flatValue = flatValue;
            _cluster = cluster;
            _shardsCount = shardsCount;
        }

        public int GetFlatValue()
        {
            return _flatValue;
        }

        public object GetLocation()
        {
            return Tuple.Create(_cluster, _shardsCount);
        }
    }

    [DataContract]
    public class LocalSelectorDescriptor<TKey, TPayload> : IMoveDescriptor
    {
        [DataMember]
        private int _flatValue;

        [DataMember]
        private Expression<Func<TKey, string, TPayload, string[]>> _destinationSelector;

        public LocalSelectorDescriptor(int flatValue, Expression<Func<TKey, string, TPayload, string[]>> destinationSelector)
        {
            _flatValue = flatValue;
            _destinationSelector = destinationSelector;
        }

        public int GetFlatValue()
        {
            return _flatValue;
        }

        public object GetLocation()
        {
            return _destinationSelector;
        }
    }

    [DataContract]
    public class GlobalAndLocalSelectorDescriptor<TKey, TPayload> : IMoveDescriptor
    {
        [DataMember]
        private int _flatValue;

        [DataMember]
        private Expression<Func<TKey, string, TPayload, string[]>> _globalDestinationSelector;

        [DataMember]
        private Expression<Func<TKey, string, TPayload, string[]>> _localDestinationSelector;

        public GlobalAndLocalSelectorDescriptor(int flatValue, Expression<Func<TKey, string, TPayload, string[]>> globalDestinationSelector,
            Expression<Func<TKey, string, TPayload, string[]>> localDestinationSelector)
        {
            _flatValue = flatValue;
            _globalDestinationSelector = globalDestinationSelector;
            _localDestinationSelector = localDestinationSelector;
        }

        public int GetFlatValue()
        {
            return _flatValue;
        }

        public object GetLocation()
        {
            return Tuple.Create(_globalDestinationSelector, _localDestinationSelector);
        }
    }

}
