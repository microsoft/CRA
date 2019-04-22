using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace CRA.ClientLibrary.DataProcessing
{
    public static class TransformUtils
    {
        public static UnaryOperatorTypes FillUnaryTransformTypes(Type tKeyI1, Type tPayloadI1, 
                 Type tDatasetI1, Type tKeyO, Type tPayloadO, Type tDatasetO)
        {
            var unaryTransformTypes = new UnaryOperatorTypes();
            unaryTransformTypes.InputKeyType = tKeyI1;
            unaryTransformTypes.InputPayloadType = tPayloadI1;
            unaryTransformTypes.InputDatasetType = tDatasetI1;

            unaryTransformTypes.OutputKeyType = tKeyO;
            unaryTransformTypes.OutputPayloadType = tPayloadO;
            unaryTransformTypes.OutputDatasetType = tDatasetO;

            return unaryTransformTypes;
        }

        public static BinaryOperatorTypes FillBinaryTransformTypes(Type tKeyI1, Type tPayloadI1, 
            Type tDatasetI1, Type tKeyI2, Type tPayloadI2, Type tDatasetI2, 
            Type tKeyO, Type tPayloadO, Type tDatasetO)
        {
            var binaryTransformTypes = new BinaryOperatorTypes();
            binaryTransformTypes.InputKeyType = tKeyI1;
            binaryTransformTypes.InputPayloadType = tPayloadI1;
            binaryTransformTypes.InputDatasetType = tDatasetI1;

            binaryTransformTypes.SecondaryKeyType = tKeyI2;
            binaryTransformTypes.SecondaryPayloadType = tPayloadI2;
            binaryTransformTypes.SecondaryDatasetType = tDatasetI2;

            binaryTransformTypes.OutputKeyType = tKeyO;
            binaryTransformTypes.OutputPayloadType = tPayloadO;
            binaryTransformTypes.OutputDatasetType = tDatasetO;

            return binaryTransformTypes;
        }

        public static OperatorTransforms MergeTwoSetsOfTransforms(OperatorTransforms leftOperand, OperatorTransforms rightOperand)
        {
            List<string> transforms = new List<string>();
            transforms.AddRange(leftOperand.Transforms);
            transforms.AddRange(rightOperand.Transforms);

            List<string> transformsOperations = new List<string>();
            transformsOperations.AddRange(leftOperand.TransformsOperations);
            transformsOperations.AddRange(rightOperand.TransformsOperations);

            List<string> transformsTypes = new List<string>();
            transformsTypes.AddRange(leftOperand.TransformsTypes);
            transformsTypes.AddRange(rightOperand.TransformsTypes);

            List<OperatorInputs> transformsInputs = new List<OperatorInputs>();
            transformsInputs.AddRange(leftOperand.TransformsInputs);
            transformsInputs.AddRange(rightOperand.TransformsInputs);

            return new OperatorTransforms(transforms, transformsOperations, transformsTypes, transformsInputs);
        }


        public static object ApplyUnaryTransformer<TKeyI, TPayloadI, TDatasetI, TKeyO, TPayloadO, TDatasetO>(object dataset, string unaryTransformer)
            where TDatasetI : IDataset<TKeyI, TPayloadI>
            where TDatasetO : IDataset<TKeyO, TPayloadO>
        {
            try
            {
                if (unaryTransformer != null)
                {
                    Expression transformer = SerializationHelper.Deserialize(unaryTransformer);
                    var compiledTransformer = Expression<Func<TDatasetI, TDatasetO>>.Lambda(transformer).Compile();
                    Delegate compiledTransformerConstructor = (Delegate)compiledTransformer.DynamicInvoke();
                    return compiledTransformerConstructor.DynamicInvoke((TDatasetI)dataset);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Error: The CRA vertex failed to apply an unary transformer for an error of type " + e.GetType() + ": " + e.ToString());
            }

            return null;
        }

        /*
        public static object ApplyUnaryTransformer<TKeyI, TPayloadI, TDatasetI, TKeyO, TPayloadO, TDatasetO>(object dataset, string unaryTransformer)
            where TDatasetI : IDataset<TKeyI, TPayloadI>
            where TDatasetO : IDataset<TKeyO, TPayloadO>
        {
            try
            {
                if (unaryTransformer != null)
                { 
                    var transformer = (Expression<Func<TDatasetI, TDatasetO>>) SerializationHelper.Deserialize(unaryTransformer);
                    var compiledTransformer = transformer.Compile();
                    return compiledTransformer((TDatasetI)dataset);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Error: The CRA vertex failed to apply an unary transformer for an error of type " + e.GetType() + ": " + e.ToString());
            }

            return null;
        }*/


        public static object ApplyBinaryTransformer<TKeyI1, TPayloadI1, TDatasetI1,
                    TKeyI2, TPayloadI2, TDatasetI2, TKeyO, TPayloadO, TDatasetO>(object dataset1, object dataset2, string binaryTransformer)
            where TDatasetI1 : IDataset<TKeyI1, TPayloadI1>
            where TDatasetI2 : IDataset<TKeyI2, TPayloadI2>
            where TDatasetO : IDataset<TKeyO, TPayloadO>
        {
            try
            {
                if (binaryTransformer != null)
                {
                    Expression transformer = SerializationHelper.Deserialize(binaryTransformer);
                    var compiledTransformer = Expression<Func<TDatasetI1, TDatasetI2, TDatasetO>>.Lambda(transformer).Compile();
                    Delegate compiledTransformerConstructor = (Delegate)compiledTransformer.DynamicInvoke();
                    return compiledTransformerConstructor.DynamicInvoke((TDatasetI1)dataset1, (TDatasetI2)dataset2);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Error: The CRA vertex failed to apply a binary transformer for an error of type " + e.GetType() + ": " + e.ToString());
            }

            return null;
        }

        /*
        public static object ApplyBinaryTransformer<TKeyI1, TPayloadI1, TDatasetI1, 
                    TKeyI2, TPayloadI2, TDatasetI2, TKeyO, TPayloadO, TDatasetO>(object dataset1, object dataset2, string binaryTransformer)
            where TDatasetI1 : IDataset<TKeyI1, TPayloadI1>
            where TDatasetI2 : IDataset<TKeyI2, TPayloadI2>
            where TDatasetO : IDataset<TKeyO, TPayloadO>
        {
            try
            {
                if (binaryTransformer != null)
                {
                    var transformer = (Expression<Func<TDatasetI1, TDatasetI2, TDatasetO>>) SerializationHelper.Deserialize(binaryTransformer);
                    var compiledTransformer = transformer.Compile();
                    return compiledTransformer((TDatasetI1)dataset1, (TDatasetI2)dataset2);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("Error: The CRA vertex failed to apply a binary transformer for an error of type " + e.GetType() + ": " + e.ToString());
            }

            return null;
        }
        */
        
        public static void PrepareTransformInputs(OperatorInputs transformInputInfo, ref Object dataset1, ref string dataset1Id, 
            ref Object dataset2, ref string dataset2Id, Dictionary<string, object> cachedDatasets)
        {
            string inputId1 = transformInputInfo.InputId1;
            string inputId2 = transformInputInfo.InputId2;

            if (inputId1 != "$" && inputId1 != null && cachedDatasets.ContainsKey(inputId1))
            {
                dataset1 = cachedDatasets[inputId1];
                dataset1Id = inputId1;
            }

            if (dataset1 == null)
            {
                if (inputId2 != "$" && inputId2 != null && cachedDatasets.ContainsKey(inputId2))
                {
                    dataset1 = cachedDatasets[inputId2];
                    dataset1Id = inputId2;
                }
                else
                {
                    dataset1 = null;
                    dataset1Id = "$";
                }
                dataset2 = null;
                dataset2Id = "$";
            }
            else
            {
                if (inputId2 != "$" && inputId2 != null && cachedDatasets.ContainsKey(inputId2))
                {
                    dataset2 = cachedDatasets[inputId2];
                    dataset2Id = inputId2;
                }
                else
                {
                    dataset2 = null;
                    dataset2Id = "$";
                }
            }
        }

    }
}
