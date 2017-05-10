using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using Microsoft.WindowsAzure.Storage.Table;

namespace CRA.ClientLibrary
{
    /// <summary>
    /// An assignment of one machine to a group
    /// </summary>
    public class ProcessTable : TableEntity
    {
        /// <summary>
        /// The time interval at which workers refresh their membership entry
        /// </summary>
        public static readonly TimeSpan HeartbeatTime = TimeSpan.FromSeconds(10);

        /// <summary>
        /// Name of the CRA instance
        /// </summary>
        public string InstanceName { get { return this.PartitionKey; } }

        /// <summary>
        /// Name of process
        /// </summary>
        public string ProcessName { get { return this.RowKey; } }

        /// <summary>
        /// Definition of process
        /// </summary>
        public string ProcessDefinition { get; set; }

        /// <summary>
        /// Name of the machine
        /// </summary>
        public string Address { get; set; }

        /// <summary>
        /// Port number
        /// </summary>
        public int Port { get; set; }

        /// <summary>
        /// Action to create process
        /// </summary>
        public string ProcessCreateAction { get; set; }

        /// <summary>
        /// Parameter to process creator
        /// </summary>
        public string ProcessParameter { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="instanceName"></param>
        /// <param name="processName"></param>
        /// <param name="processDefinition"></param>
        /// <param name="address"></param>
        /// <param name="port"></param>
        /// <param name="processCreateAction"></param>
        /// <param name="processParameter"></param>
        public ProcessTable(string instanceName, string processName, string processDefinition, string address, int port, Expression<Func<IProcess>> processCreateAction, object processParameter)
        {
            this.PartitionKey = instanceName;
            this.RowKey = processName;

            this.ProcessDefinition = processDefinition;
            this.Address = address;
            this.Port = port;
            this.ProcessCreateAction = "";

            if (processCreateAction != null)
            {
                var closureEliminator = new ClosureEliminator();
                Expression processedUserLambdaExpression = closureEliminator.Visit(
                        processCreateAction);

                this.ProcessCreateAction = SerializationHelper.Serialize(processedUserLambdaExpression);
            }

            this.ProcessParameter = SerializationHelper.SerializeObject(processParameter);
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="instanceName"></param>
        /// <param name="processName"></param>
        /// <param name="processDefinition"></param>
        /// <param name="address"></param>
        /// <param name="port"></param>
        /// <param name="processCreateAction"></param>
        /// <param name="processParameter"></param>
        public ProcessTable(string instanceName, string processName, string processDefinition, string address, int port, string processCreateAction, string processParameter)
        {
            this.PartitionKey = instanceName;
            this.RowKey = processName;

            this.ProcessDefinition = processDefinition;
            this.Address = address;
            this.Port = port;
            this.ProcessCreateAction = processCreateAction;
            this.ProcessParameter = processParameter;
        }

        /// <summary>
        /// Constructor
        /// </summary>
        public ProcessTable() { }

        /// <summary>
        /// ToString
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return string.Format(CultureInfo.CurrentCulture, "Instance '{0}', Address '{1}', Port '{2}'", this.InstanceName, this.Address, this.Port);
        }

        /// <summary>
        /// Equals
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object obj)
        {
            ProcessTable other = obj as ProcessTable;
            return this.PartitionKey.Equals(other.PartitionKey) && this.RowKey.Equals(other.RowKey);
        }

        /// <summary>
        /// GetHashCode
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            return PartitionKey.GetHashCode() ^ RowKey.GetHashCode();
        }

        internal Func<IProcess> GetProcessCreateAction()
        {
            var expr = SerializationHelper.Deserialize(ProcessCreateAction);
            var actionExpr = AddBox((LambdaExpression) expr);
            return actionExpr.Compile();
        }

        internal object GetProcessParam()
        {
            return SerializationHelper.DeserializeObject(this.ProcessParameter);
        }


        private static Expression<Func<IProcess>> AddBox(LambdaExpression expression)
        {
            Expression converted = Expression.Convert
                 (expression.Body, typeof(IProcess));
            return Expression.Lambda<Func<IProcess>>
                 (converted, expression.Parameters);
        }

        /// <summary>
        /// Returns a list of all visible nodes in all groups
        /// </summary>
        /// <param name="instanceTable"></param>
        /// <returns></returns>
        internal static IEnumerable<ProcessTable> GetAll(CloudTable instanceTable)
        {
            TableQuery<ProcessTable> query = new TableQuery<ProcessTable>();
            return instanceTable.ExecuteQuery(query);
        }

        /// <summary>
        /// Counts all nodes in the cluster regardless of their group
        /// </summary>
        /// <returns></returns>
        internal static int CountAll(CloudTable instanceTable)
        {
            return GetAll(instanceTable).Count();
        }

        internal static ProcessTable GetInstanceFromAddress(CloudTable instanceTable, string address, int port)
        {
            return GetAll(instanceTable).Where(gn => address == gn.Address && port == gn.Port).First();
        }

        internal static ProcessTable GetRowForInstance(CloudTable instanceTable, string instanceName)
        {
            return GetAll(instanceTable).Where(gn => instanceName == gn.InstanceName && string.IsNullOrEmpty(gn.ProcessName)).First();
        }
        internal static IEnumerable<ProcessTable> GetAllRowsForInstance(CloudTable instanceTable, string instanceName)
        {
            return GetAll(instanceTable).Where(gn => instanceName == gn.InstanceName);
        }

        internal static ProcessTable GetRowForInstanceProcess(CloudTable instanceTable, string instanceName, string processName)
        {
            return GetAll(instanceTable).Where(gn => instanceName == gn.InstanceName && processName == gn.ProcessName).First();
        }

        internal static ProcessTable GetRowForProcessDefinition(CloudTable instanceTable, string processDefinition)
        {
            return GetAll(instanceTable).Where(gn => processDefinition == gn.ProcessName && string.IsNullOrEmpty(gn.InstanceName)).First();
        }

        internal static ProcessTable GetRowForProcess(CloudTable instanceTable, string processName)
        {
            return GetAll(instanceTable).Where(gn => processName == gn.ProcessName && !string.IsNullOrEmpty(gn.InstanceName)).First();
        }

        internal static IEnumerable<ProcessTable> GetProcesses(CloudTable instanceTable, string instanceName)
        {
            return GetAll(instanceTable).Where(gn => instanceName == gn.InstanceName && !string.IsNullOrEmpty(gn.ProcessName));
        }

        internal static bool ContainsRow(CloudTable instanceTable, ProcessTable entity)
        {
            var temp = GetAll(instanceTable);

            return temp.Where(gn => entity.Equals(gn)).Count() > 0;
        }

        internal static bool ContainsInstance(CloudTable instanceTable, string instanceName)
        {
            var temp = GetAll(instanceTable);

            return temp.Where(gn => instanceName == gn.InstanceName).Count() > 0;
        }

    }
}
