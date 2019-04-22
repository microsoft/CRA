namespace CRA.ClientLibrary.DataProcessing
{
    public static class OperatorUtils
    {
        public static string[] PrepareOutputEndpointsIdsForOperator(string toOutputId, OperatorEndpointsDescriptor endpointsDescriptor)
        {
            if (endpointsDescriptor.ToOutputs.Count != 0)
            {
                int outputEndpointsTotal = endpointsDescriptor.ToOutputs[toOutputId];
                string[] outputEndpoints = new string[outputEndpointsTotal];
                for (int i = 0; i < outputEndpointsTotal; i++)
                    outputEndpoints[i] = "OutputTo" + toOutputId + i;
                return outputEndpoints;
            }

            return null;
        }

        public static string PrepareOutputEndpointIdForOperator(string toOutputId, OperatorEndpointsDescriptor endpointsDescriptor)
        {
            if (endpointsDescriptor.ToOutputs.Count != 0)
                return "OutputTo" + toOutputId;

            return null;
        }

        public static string[] PrepareInputEndpointsIdsForOperator(string fromInputId, OperatorEndpointsDescriptor endpointsDescriptor)
        {
            if (endpointsDescriptor.FromInputs.Count != 0)
            {
                int inputEndpointsTotal = endpointsDescriptor.FromInputs[fromInputId];
                string[] inputEndpoints = new string[inputEndpointsTotal];
                for (int i = 0; i < inputEndpointsTotal; i++)
                     inputEndpoints[i] = "InputFrom" + fromInputId + i;
                return inputEndpoints;
            }

            return null;
        }

        public static string PrepareInputEndpointIdForOperator(string fromInputId, OperatorEndpointsDescriptor endpointsDescriptor)
        {
            if (endpointsDescriptor.FromInputs.Count != 0)
                return "InputFrom" + fromInputId;

            return null;
        }
    }
}
