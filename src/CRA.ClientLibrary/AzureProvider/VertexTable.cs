using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.IO;
using System.Configuration;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using CRA.ClientLibrary;
using System.Threading.Tasks;

namespace CRA.DataProvider.Azure
{
    /// <summary>
    /// An assignment of one machine to a group
    /// </summary>
    public class VertexTable : TableEntity
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
        /// Name of vertex
        /// </summary>
        public string VertexName { get { return this.RowKey; } }

        /// <summary>
        /// Definition of vertex
        /// </summary>
        public string VertexDefinition { get; set; }

            /// <summary>
        /// IP address of the machine
        /// </summary>
        public string Address { get; set; }

        /// <summary>
        /// Port number
        /// </summary>
        public int Port { get; set; }

        /// <summary>
        /// Action to create vertex
        /// </summary>
        public string VertexCreateAction { get; set; }

        /// <summary>
        /// Parameter to vertex creator
        /// </summary>
        public string VertexParameter { get; set; }

        /// <summary>
        /// Whether the vertex is the "active" vertex
        /// (only one vertex can be active at a time, even
        /// if many vertex copies exist on different instances)
        /// </summary>
        public bool IsActive { get; set; }

        /// <summary>
        /// Whether the vertex is of sharded type
        /// </summary>
        public bool IsSharded { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="instanceName"></param>
        /// <param name="vertexName"></param>
        /// <param name="vertexDefinition"></param>
        /// <param name="address"></param>
        /// <param name="port"></param>
        /// <param name="vertexCreateAction"></param>
        /// <param name="vertexParameter"></param>
        /// <param name="isActive"></param>
        public VertexTable(string instanceName, string vertexName, string vertexDefinition, string address, int port, Expression<Func<IVertex>> vertexCreateAction, object vertexParameter, bool isActive)
        {
            this.PartitionKey = instanceName;
            this.RowKey = vertexName;

            this.VertexDefinition = vertexDefinition;
            this.Address = address;
            this.Port = port;
            this.VertexCreateAction = "";
            this.IsActive = isActive;
            this.IsSharded = false;

            if (vertexCreateAction != null)
            {
                var closureEliminator = new ClosureEliminator();
                Expression vertexedUserLambdaExpression = closureEliminator.Visit(
                        vertexCreateAction);

                this.VertexCreateAction = SerializationHelper.Serialize(vertexedUserLambdaExpression);
            }

            this.VertexParameter = SerializationHelper.SerializeObject(vertexParameter);
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="instanceName"></param>
        /// <param name="vertexName"></param>
        /// <param name="vertexDefinition"></param>
        /// <param name="address"></param>
        /// <param name="port"></param>
        /// <param name="vertexCreateAction"></param>
        /// <param name="vertexParameter"></param>
        /// <param name="isActive"></param>
        public VertexTable(string instanceName, string vertexName, string vertexDefinition, string address, int port, Expression<Func<IShardedVertex>> vertexCreateAction, object vertexParameter, bool isActive)
        {
            this.PartitionKey = instanceName;
            this.RowKey = vertexName;

            this.VertexDefinition = vertexDefinition;
            this.Address = address;
            this.Port = port;
            this.VertexCreateAction = "";
            this.IsActive = isActive;
            this.IsSharded = true;

            if (vertexCreateAction != null)
            {
                var closureEliminator = new ClosureEliminator();
                Expression vertexedUserLambdaExpression = closureEliminator.Visit(
                        vertexCreateAction);

                this.VertexCreateAction = SerializationHelper.Serialize(vertexedUserLambdaExpression);
            }

            this.VertexParameter = SerializationHelper.SerializeObject(vertexParameter);
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="instanceName"></param>
        /// <param name="vertexName"></param>
        /// <param name="vertexDefinition"></param>
        /// <param name="address"></param>
        /// <param name="port"></param>
        /// <param name="vertexCreateAction"></param>
        /// <param name="vertexParameter"></param>
        /// <param name="isActive"></param>
        public VertexTable(string instanceName, string vertexName, string vertexDefinition, string address, int port, string vertexCreateAction, string vertexParameter, bool isActive, bool isSharded)
        {
            this.PartitionKey = instanceName;
            this.RowKey = vertexName;

            this.VertexDefinition = vertexDefinition;
            this.Address = address;
            this.Port = port;
            this.VertexCreateAction = vertexCreateAction;
            this.VertexParameter = vertexParameter;
            this.IsActive = isActive;
            this.IsSharded = isSharded;
        }

        /// <summary>
        /// Constructor
        /// </summary>
        public VertexTable() { }

        public static implicit operator VertexTable(VertexInfo vertexInfo)
            => new VertexTable(
                instanceName: vertexInfo.InstanceName,
                vertexName: vertexInfo.VertexName,
                vertexDefinition: vertexInfo.VertexDefinition,
                address: vertexInfo.Address,
                port: vertexInfo.Port,
                vertexCreateAction: vertexInfo.VertexCreateAction,
                vertexParameter: vertexInfo.VertexParameter,
                isActive: vertexInfo.IsActive,
                isSharded: vertexInfo.IsSharded)
            { ETag = vertexInfo.VersionId };

        public static implicit operator VertexInfo(VertexTable vertexInfo)
            => new VertexInfo(
                instanceName: vertexInfo.InstanceName,
                vertexName: vertexInfo.VertexName,
                vertexDefinition: vertexInfo.VertexDefinition,
                address: vertexInfo.Address,
                port: vertexInfo.Port,
                vertexCreateAction: vertexInfo.VertexCreateAction,
                vertexParameter: vertexInfo.VertexParameter,
                isActive: vertexInfo.IsActive,
                isSharded: vertexInfo.IsSharded,
                versionId: vertexInfo.ETag);

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
            VertexTable other = obj as VertexTable;
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

        internal Func<IVertex> GetVertexCreateAction()
        {
            var expr = SerializationHelper.Deserialize(VertexCreateAction);
            var actionExpr = AddBox((LambdaExpression)expr);
            return actionExpr.Compile();
        }

        internal async Task<object> GetVertexParam()
        {
            string storageConnectionString = null;
#if !DOTNETCORE
            storageConnectionString = ConfigurationManager.AppSettings.Get("AZURE_STORAGE_CONN_STRING");
#endif
            if (storageConnectionString == null)
                storageConnectionString = Environment.GetEnvironmentVariable("AZURE_STORAGE_CONN_STRING");
            if (storageConnectionString == null)
                throw new InvalidOperationException("Azure storage connection string not found. Use appSettings in your app.config to provide this using the key AZURE_STORAGE_CONN_STRING, or use the environment variable AZURE_STORAGE_CONN_STRING.");

            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            string blobName = VertexName + "-" + InstanceName;

            CloudBlobContainer container = blobClient.GetContainerReference("cra");
            await container.CreateIfNotExistsAsync();
            var blockBlob = container.GetBlockBlobReference(VertexDefinition + "/" + blobName);
            Stream blobStream = blockBlob.OpenReadAsync().GetAwaiter().GetResult();
            byte[] parameterBytes = blobStream.ReadByteArray();
            string parameterString = Encoding.UTF8.GetString(parameterBytes);
            blobStream.Close();

            return SerializationHelper.DeserializeObject(parameterString);
        }


        private static Expression<Func<IVertex>> AddBox(LambdaExpression expression)
        {
            Expression converted = Expression.Convert
                 (expression.Body, typeof(IVertex));
            return Expression.Lambda<Func<IVertex>>
                 (converted, expression.Parameters);
        }

        /// <summary>
        /// Returns a list of all visible nodes in all groups
        /// </summary>
        /// <param name="instanceTable"></param>
        /// <returns></returns>
        internal static IEnumerable<VertexTable> GetAll(CloudTable instanceTable)
        {
            TableQuery<VertexTable> query = new TableQuery<VertexTable>();
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

        internal static VertexTable GetInstanceFromAddress(CloudTable instanceTable, string address, int port)
        {
            return GetAll(instanceTable).Where(gn => address == gn.Address && port == gn.Port).First();
        }

        internal static VertexTable GetRowForInstance(CloudTable instanceTable, string instanceName)
        {
            return GetAll(instanceTable).Where(gn => instanceName == gn.InstanceName && string.IsNullOrEmpty(gn.VertexName)).First();
        }
        internal static IEnumerable<VertexTable> GetAllRowsForInstance(CloudTable instanceTable, string instanceName)
        {
            return GetAll(instanceTable).Where(gn => instanceName == gn.InstanceName);
        }

        internal static VertexTable GetRowForInstanceVertex(CloudTable instanceTable, string instanceName, string vertexName)
        {
            return GetAll(instanceTable).Where(gn => instanceName == gn.InstanceName && vertexName == gn.VertexName).First();
        }

        internal static VertexTable GetRowForVertexDefinition(CloudTable instanceTable, string vertexDefinition)
        {
            return GetAll(instanceTable).Where(gn => vertexDefinition == gn.VertexName && string.IsNullOrEmpty(gn.InstanceName)).First();
        }

        internal static VertexTable GetRowForVertex(CloudTable instanceTable, string vertexName)
        {
            return GetAll(instanceTable)
                .Where(gn => vertexName == gn.VertexName && !string.IsNullOrEmpty(gn.InstanceName))
                .Where(gn => gn.IsActive)
                .First();
        }

        internal static IEnumerable<VertexTable> GetVertices(CloudTable instanceTable, string instanceName)
        {
            return GetAll(instanceTable).Where(gn => instanceName == gn.InstanceName && !string.IsNullOrEmpty(gn.VertexName));
        }

        internal static IEnumerable<VertexTable> GetRowsForShardedVertex(CloudTable instanceTable, string vertexName)
        {
            return GetAll(instanceTable).Where(gn => gn.VertexName.StartsWith(vertexName + "$") && !string.IsNullOrEmpty(gn.VertexName));
        }

        internal static bool ContainsRow(CloudTable instanceTable, VertexTable entity)
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
