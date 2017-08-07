using System;
using System.Runtime.Serialization;
using System.Text;

namespace CRA.ClientLibrary.DataProcessing
{
    [DataContract]
    public class BinaryOperatorTypes
    {
        // First input dataset information
        [DataMember]
        public Type InputKeyType { get; set; }

        [DataMember]
        public Type InputPayloadType { get; set; }

        [DataMember]
        public Type InputDatasetType { get; set; }

        // Second input dataset information
        [DataMember]
        public Type SecondaryKeyType { get; set; }

        [DataMember]
        public Type SecondaryPayloadType { get; set; }

        [DataMember]
        public Type SecondaryDatasetType { get; set; }

        // Output dataset information
        [DataMember]
        public Type OutputKeyType { get; set; }

        [DataMember]
        public Type OutputPayloadType { get; set; }

        [DataMember]
        public Type OutputDatasetType { get; set; }


        public void SetInputKeyType(Type inputKeyType)
        {
            InputKeyType = inputKeyType;
        }

        public void SetInputPayloadType(Type inputPayloadType)
        {
            InputPayloadType = inputPayloadType;
        }

        public void SetInputDatasetType(Type inputDatasetType)
        {
            InputDatasetType = inputDatasetType;
        }

        public void SetSecondaryKeyType(Type secondaryKeyType)
        {
            SecondaryKeyType = secondaryKeyType;
        }

        public void SetSecondaryPayloadType(Type secondaryPayloadType)
        {
            SecondaryPayloadType = secondaryPayloadType;
        }

        public void SetSecondaryDatasetType(Type secondaryDatasetType)
        {
            SecondaryDatasetType = secondaryDatasetType;
        }

        public void SetOutputKeyType(Type outputKeyType)
        {
            OutputKeyType = outputKeyType;
        }

        public void SetOutputPayloadType(Type outputPayloadType)
        {
            OutputPayloadType = outputPayloadType;
        }

        public void SetOutputDatasetType(Type outputDatasetType)
        {
            OutputDatasetType = outputDatasetType;
        }

        public override string ToString()
        {
            if (InputKeyType == null)
            {
                return "";
            }
            else
            {
                return InputKeyType.AssemblyQualifiedName + ":" +
                       InputPayloadType.AssemblyQualifiedName + ":" +
                       InputDatasetType.AssemblyQualifiedName + ":" +
                       SecondaryKeyType.AssemblyQualifiedName + ":" +
                       SecondaryPayloadType.AssemblyQualifiedName + ":" +
                       SecondaryDatasetType.AssemblyQualifiedName + ":" +
                       OutputKeyType.AssemblyQualifiedName + ":" +
                       OutputPayloadType.AssemblyQualifiedName + ":" +
                       OutputDatasetType.AssemblyQualifiedName;
            }
        }

        public void FromString(string types)
        {
            if (types.Length == 0)
            {
                InputKeyType = null;
                InputPayloadType = null;
                InputDatasetType = null;
                SecondaryKeyType = null;
                SecondaryPayloadType = null;
                SecondaryDatasetType = null;
                OutputKeyType = null;
                OutputPayloadType = null;
                OutputDatasetType = null;
            }
            else
            {
                string[] information = types.Split(new char[] { ':' }, 9);
                InputKeyType = Type.GetType(information[0]);
                InputPayloadType = Type.GetType(information[1]);
                InputDatasetType = Type.GetType(information[2]);
                SecondaryKeyType = Type.GetType(information[3]);
                SecondaryPayloadType = Type.GetType(information[4]);
                SecondaryDatasetType = Type.GetType(information[5]);
                OutputKeyType = Type.GetType(information[6]);
                OutputPayloadType = Type.GetType(information[7]);
                OutputDatasetType = Type.GetType(information[8]);
            }

        }
    }

    [DataContract]
    public class UnaryOperatorTypes
    {
        // Input dataset information
        [DataMember]
        public Type InputKeyType { get; set; }

        [DataMember]
        public Type InputPayloadType { get; set; }

        [DataMember]
        public Type InputDatasetType { get; set; }

        // Output dataset information
        [DataMember]
        public Type OutputKeyType { get; set; }

        [DataMember]
        public Type OutputPayloadType { get; set; }

        [DataMember]
        public Type OutputDatasetType { get; set; }

        public void SetInputKeyType(Type inputKeyType)
        {
            InputKeyType = inputKeyType;
        }

        public void SetInputPayloadType(Type inputPayloadType)
        {
            InputPayloadType = inputPayloadType;
        }

        public void SetInputDatasetType(Type inputDatasetType)
        {
            InputDatasetType = inputDatasetType;
        }

        public void SetOutputKeyType(Type outputKeyType)
        {
            OutputKeyType = outputKeyType;
        }

        public void SetOutputPayloadType(Type outputPayloadType)
        {
            OutputPayloadType = outputPayloadType;
        }

        public void SetOutputDatasetType(Type outputDatasetType)
        {
            OutputDatasetType = outputDatasetType;
        }

        public override string ToString()
        {
            if (InputKeyType == null)
            {
                return "";
            }
            else

                return InputKeyType.AssemblyQualifiedName + ":" +
                       InputPayloadType.AssemblyQualifiedName + ":" +
                       InputDatasetType.AssemblyQualifiedName + ":" +
                       OutputKeyType.AssemblyQualifiedName + ":" +
                       OutputPayloadType.AssemblyQualifiedName + ":" +
                       OutputDatasetType.AssemblyQualifiedName;
        }

        public void FromString(string types)
        {
            if (types.Length == 0)
            {
                InputKeyType = null;
                InputPayloadType = null;
                InputDatasetType = null;
                OutputKeyType = null;
                OutputPayloadType = null;
                OutputDatasetType = null;
            }
            else
            {
                string[] information = types.Split(new char[] { ':' }, 6);
                InputKeyType = Type.GetType(information[0]);
                InputPayloadType = Type.GetType(information[1]);
                InputDatasetType = Type.GetType(information[2]);
                OutputKeyType = Type.GetType(information[3]);
                OutputPayloadType = Type.GetType(information[4]);
                OutputDatasetType = Type.GetType(information[5]);
            }

        }
    }

    [DataContract]
    public class OperatorInputs
    {
        [DataMember]
        public string InputId1 { get; set; }

        [DataMember]
        public string InputId2 { get; set; }

        public void SetInputId1(string inputId1)
        {
            InputId1 = inputId1;
        }

        public void SetInputId2(string inputId2)
        {
            InputId2 = inputId2;
        }

        public override string ToString()
        {
            if (InputId1 == null  && InputId2 == null)
            {
                return "";
            }
            else
            {
                StringBuilder builder = new StringBuilder();
                if (InputId1 != null)  
                    builder.Append(InputId1 + ":");
                else
                    builder.Append("$:");

                if (InputId2 != null)
                    builder.Append(InputId2);
                else
                    builder.Append("$");

                return builder.ToString();
            }
        }

        public OperatorInputs FromString(string inputs)
        {
            if (inputs.Length == 0)
            {
                InputId1 = null;
                InputId2 = null;
            }
            else
            {
                string[] info = inputs.Split(new char[] { ':' }, 2);
                InputId1 = info[0];
                InputId2 = info[1];
            }
            return this;
        }
    }
}
