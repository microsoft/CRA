using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq.Expressions;
using System.Net.Sockets;
using System.Text;

namespace CRA.ClientLibrary
{
    /// <summary>
    /// Client library for Common Runtime for Applications (CRA)
    /// </summary>
    public partial class CRAClientLibrary
    {
        CRAWorker _localWorker;

        // Azure storage clients
        string _storageConnectionString;
        CloudStorageAccount _storageAccount;

        CloudBlobClient _blobClient;
        CloudTableClient _tableClient;

        CloudTable _processTable;
        CloudTable _connectionTable;

        internal ProcessTableManager _processTableManager;
        EndpointTableManager _endpointTableManager;
        ConnectionTableManager _connectionTableManager;

        Type aquaType = typeof(Aqua.TypeSystem.ConstructorInfo);

        /// <summary>
        /// Create an instance of the client library for Common Runtime for Applications (CRA)
        /// </summary>
        public CRAClientLibrary() : this("", null)
        {
        }

        /// <summary>
        /// Create an instance of the client library for Common Runtime for Applications (CRA)
        /// </summary>
        /// <param name="storageConnectionString">Optional storage account to use for CRA metadata, if
        /// not specified, it will use the appSettings key named StorageConnectionString in app.config</param>
        public CRAClientLibrary(string storageConnectionString) : this(storageConnectionString, null)
        {
        }

        /// <summary>
        /// Create an instance of the client library for Common Runtime for Applications (CRA)
        /// </summary>
        /// <param name="storageConnectionString">Optional storage account to use for CRA metadata, if
        /// not specified, it will use the appSettings key named StorageConnectionString in app.config</param>
        /// <param name = "localWorker" >Local worker if any</param>
        public CRAClientLibrary(string storageConnectionString, CRAWorker localWorker)
        {
            _localWorker = localWorker;

            if (storageConnectionString == "" || storageConnectionString == null)
            {
                _storageConnectionString = ConfigurationManager.AppSettings.Get("CRA_STORAGE_CONN_STRING");
                if (_storageConnectionString == null)
                {
                    _storageConnectionString = Environment.GetEnvironmentVariable("CRA_STORAGE_CONN_STRING");
                }
                if (_storageConnectionString == null)
                {
                    throw new InvalidOperationException("CRA storage connection string not found. Use appSettings in your app.config to provide this using the key CRA_STORAGE_CONN_STRING, or use the environment variable CRA_STORAGE_CONN_STRING.");
                }
            }
            else
                _storageConnectionString = storageConnectionString;

            _storageAccount = CloudStorageAccount.Parse(_storageConnectionString);

            _blobClient = _storageAccount.CreateCloudBlobClient();
            _tableClient = _storageAccount.CreateCloudTableClient();

            _processTableManager = new ProcessTableManager(_storageConnectionString);
            _endpointTableManager = new EndpointTableManager(_storageConnectionString);
            _connectionTableManager = new ConnectionTableManager(_storageConnectionString);

            _processTable = CreateTableIfNotExists("processtableforcra");
            _connectionTable = CreateTableIfNotExists("connectiontableforcra");
        }

        /// <summary>
        /// Define a process type and register with CRA.
        /// </summary>
        /// <param name="processDefinition">Name of the process type</param>
        /// <param name="creator">Lambda that describes how to instantiate the process, taking in an object as parameter</param>
        public CRAErrorCode DefineProcess(string processDefinition, Expression<Func<IProcess>> creator)
        {
            CloudBlobContainer container = _blobClient.GetContainerReference("cra");
            container.CreateIfNotExists();
            var blockBlob = container.GetBlockBlobReference(processDefinition + "/binaries");
            CloudBlobStream blobStream = blockBlob.OpenWrite();
            AssemblyUtils.WriteAssembliesToStream(blobStream);
            blobStream.Close();

            // Add metadata
            var newRow = new ProcessTable("", processDefinition, processDefinition, "", 0, creator, null);
            TableOperation insertOperation = TableOperation.InsertOrReplace(newRow);
            _processTable.Execute(insertOperation);

            return CRAErrorCode.Success;
        }

        /// <summary>
        /// Resets the cluster and deletes all knowledge of any CRA instances
        /// </summary>
        public void Reset()
        {
            _connectionTable.DeleteIfExists();
            _processTable.DeleteIfExists();
            _endpointTableManager.DeleteTable();
        }

        /// <summary>
        /// Delete contents of a cloud table
        /// </summary>
        /// <param name="_table"></param>
        private static void DeleteContents(CloudTable table)
        {
            Action<IEnumerable<DynamicTableEntity>> processor = entities =>
            {
                var batches = new Dictionary<string, TableBatchOperation>();

                foreach (var entity in entities)
                {
                    TableBatchOperation batch = null;

                    if (batches.TryGetValue(entity.PartitionKey, out batch) == false)
                    {
                        batches[entity.PartitionKey] = batch = new TableBatchOperation();
                    }

                    batch.Add(TableOperation.Delete(entity));

                    if (batch.Count == 100)
                    {
                        table.ExecuteBatch(batch);
                        batches[entity.PartitionKey] = new TableBatchOperation();
                    }
                }

                foreach (var batch in batches.Values)
                {
                    if (batch.Count > 0)
                    {
                        table.ExecuteBatch(batch);
                    }
                }
            };

            ProcessEntities(table, processor);
        }

        /// <summary>
        /// Process all entities in a cloud table using the given processor lambda.
        /// </summary>
        /// <param name="table"></param>
        /// <param name="processor"></param>
        private static void ProcessEntities(CloudTable table, Action<IEnumerable<DynamicTableEntity>> processor)
        {
            TableQuerySegment<DynamicTableEntity> segment = null;

            while (segment == null || segment.ContinuationToken != null)
            {
                segment = table.ExecuteQuerySegmented(new TableQuery().Take(100), segment == null ? null : segment.ContinuationToken);
                processor(segment.Results);
            }
        }

        /// <summary>
        /// Not yet implemented
        /// </summary>
        /// <param name="instanceName"></param>
        public void DeployInstance(string instanceName)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Instantiate a process on a CRA instance.
        /// </summary>
        /// <param name="instanceName">Name of the CRA instance on which process is instantiated</param>
        /// <param name="processName">Name of the process (particular instance)</param>
        /// <param name="processDefinition">Definition of the process (type)</param>
        /// <param name="processParameter">Parameters to be passed to the process in its constructor (serializable object)</param>
        /// <returns>Status of the command</returns>
        public CRAErrorCode InstantiateProcess(string instanceName, string processName, string processDefinition, object processParameter)
        {
            var procDefRow = ProcessTable.GetRowForProcessDefinition(_processTable, processDefinition);

            // Serialize and write the process parameters to a blob
            CloudBlobContainer container = _blobClient.GetContainerReference("cra");
            container.CreateIfNotExists();
            var blockBlob = container.GetBlockBlobReference(processDefinition + "/" + processName);
            CloudBlobStream blobStream = blockBlob.OpenWrite();
            byte[] parameterBytes = Encoding.UTF8.GetBytes(
                        SerializationHelper.SerializeObject(processParameter));
            blobStream.WriteByteArray(parameterBytes);
            blobStream.Close();

            // Add metadata
            var newRow = new ProcessTable(instanceName, processName, processDefinition, "", 0,
                procDefRow.ProcessCreateAction, processName);
            TableOperation insertOperation = TableOperation.InsertOrReplace(newRow);
            _processTable.Execute(insertOperation);

            CRAErrorCode result = CRAErrorCode.Success;

            // Send request to CRA instance
            ProcessTable instanceRow;
            try
            {
                instanceRow = ProcessTable.GetRowForInstance(_processTable, instanceName);

                // Get a stream connection from the pool if available
                NetworkStream stream;
                if (!TryGetSenderStreamFromPool(instanceRow.Address, instanceRow.Port.ToString(), out stream))
                {
                    TcpClient client = new TcpClient(instanceRow.Address, instanceRow.Port);
                    client.NoDelay = true;

                    stream = client.GetStream();
                }

                stream.WriteInt32((int)CRATaskMessageType.LOAD_PROCESS);
                stream.WriteByteArray(Encoding.UTF8.GetBytes(processName));
                stream.WriteByteArray(Encoding.UTF8.GetBytes(processDefinition));
                stream.WriteByteArray(Encoding.UTF8.GetBytes(newRow.ProcessParameter));
                result = (CRAErrorCode)stream.ReadInt32();
                if (result != 0)
                {
                    Console.WriteLine("Process was logically loaded. However, we received an error code from the hosting CRA instance: " + result);
                }

                // Add/Return stream connection to the pool
                TryAddSenderStreamToPool(instanceRow.Address, instanceRow.Port.ToString(), stream);
            }
            catch
            {
                Console.WriteLine("The CRA instance appears to be down. Restart it and this process will be instantiated automatically");
            }
            return result;
        }

        /// <summary>
        /// Register caller as a process with given name, dummy temp instance
        /// </summary>
        /// <param name="processName"></param>
        /// <returns></returns>
        public DetachedProcess RegisterAsProcess(string processName)
        {
            return new DetachedProcess(processName, "", this);
        }

        /// <summary>
        /// Register caller as a process with given name, given CRA instance name
        /// </summary>
        /// <param name="processName"></param>
        /// <param name="instanceName"></param>
        /// <returns></returns>
        public DetachedProcess RegisterAsProcess(string processName, string instanceName)
        {
            return new DetachedProcess(processName, instanceName, this);
        }

        /// <summary>
        /// Register CRA instance name
        /// </summary>
        /// <param name="instanceName"></param>
        /// <param name="address"></param>
        /// <param name="port"></param>
        public void RegisterInstance(string instanceName, string address, int port)
        {
            _processTableManager.RegisterInstance(instanceName, address, port);
        }

        /// <summary>
        /// Delete CRA instance name
        /// </summary>
        /// <param name="instanceName"></param>
        public void DeleteInstance(string instanceName)
        {
            _processTableManager.DeleteInstance(instanceName);
        }

        /// <summary>
        /// Delete process with given name
        /// </summary>
        /// <param name="processName"></param>
        /// <param name="instanceName"></param>
        public void DeleteProcess(string processName)
        {
            foreach (var endpt in GetInputEndpoints(processName))
            {
                DeleteEndpoint(processName, endpt);
            }
            foreach (var endpt in GetOutputEndpoints(processName))
            {
                DeleteEndpoint(processName, endpt);
            }

            foreach (var conn in GetConnectionsFromProcess(processName))
            {
                DeleteConnectionInfo(conn);
            }
            foreach (var conn in GetConnectionsToProcess(processName))
            {
                DeleteConnectionInfo(conn);
            }

            var query = new TableQuery<ProcessTable>()
                   .Where(TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, processName));

            foreach (var item in _processTable.ExecuteQuery(query))
            {
                var oper = TableOperation.Delete(item);
                _processTable.Execute(oper);
            }
        }

        /// <summary>
        /// Delete process definition with given name
        /// </summary>
        /// <param name="processDefinition"></param>
        public void DeleteProcessDefinition(string processDefinition)
        {
            var entity = new DynamicTableEntity("", processDefinition);
            entity.ETag = "*";
            TableOperation deleteOperation = TableOperation.Delete(entity);
            _processTable.Execute(deleteOperation);
            CloudBlobContainer container = _blobClient.GetContainerReference("cra");
            container.CreateIfNotExists();
            var blockBlob = container.GetBlockBlobReference(processDefinition + "/binaries");
            blockBlob.DeleteIfExists();
        }


        /// <summary>
        /// Add endpoint to the appropriate CRA metadata table
        /// </summary>
        /// <param name="processName"></param>
        /// <param name="endpointName"></param>
        /// <param name="isInput"></param>
        /// <param name="isAsync"></param>
        public void AddEndpoint(string processName, string endpointName, bool isInput, bool isAsync)
        {
            _endpointTableManager.AddEndpoint(processName, endpointName, isInput, isAsync);
        }

        /// <summary>
        /// Delete endpoint
        /// </summary>
        /// <param name="processName"></param>
        /// <param name="endpointName"></param>
        public void DeleteEndpoint(string processName, string endpointName)
        {
            _endpointTableManager.DeleteEndpoint(processName, endpointName);
        }

        /// <summary>
        /// Load a process on the local instance
        /// </summary>
        /// <param name="processName"></param>
        /// <param name="processDefinition"></param>
        /// <param name="processParameter"></param>
        /// <param name="instanceName"></param>
        /// <param name="table"></param>
        /// <returns></returns>
        public IProcess LoadProcess(string processName, string processDefinition, string processParameter, string instanceName, ConcurrentDictionary<string, IProcess> table)
        {
            CloudBlobContainer container = _blobClient.GetContainerReference("cra");
            container.CreateIfNotExists();
            var blockBlob = container.GetBlockBlobReference(processDefinition + "/binaries");
            Stream blobStream = blockBlob.OpenRead();
            AssemblyUtils.LoadAssembliesFromStream(blobStream);
            AssemblyUtils.DumpAssemblies();
            blobStream.Close();

            var row = ProcessTable.GetRowForProcessDefinition(_processTable, processDefinition);

            // CREATE THE PROCESS
            var process = row.GetProcessCreateAction()();

            // LATCH CALLBACKS TO POPULATE ENDPOINT TABLE
            process.OnAddInputEndpoint((name, endpt) => _endpointTableManager.AddEndpoint(processName, name, true, false));
            process.OnAddOutputEndpoint((name, endpt) => _endpointTableManager.AddEndpoint(processName, name, false, false));
            process.OnAddAsyncInputEndpoint((name, endpt) => _endpointTableManager.AddEndpoint(processName, name, true, true));
            process.OnAddAsyncOutputEndpoint((name, endpt) => _endpointTableManager.AddEndpoint(processName, name, false, true));

            //ADD TO TABLE
            if (table != null)
            {
                table.AddOrUpdate(processName, process, (procName, oldProc) => { oldProc.Dispose(); return process; });

                process.OnDispose(() =>
                {
                    // Delete all endpoints of the process
                    foreach (var key in process.InputEndpoints)
                    {
                        _endpointTableManager.DeleteEndpoint(processName, key.Key);
                    }
                    foreach (var key in process.AsyncInputEndpoints)
                    {
                        _endpointTableManager.DeleteEndpoint(processName, key.Key);
                    }
                    foreach (var key in process.OutputEndpoints)
                    {
                        _endpointTableManager.DeleteEndpoint(processName, key.Key);
                    }
                    foreach (var key in process.AsyncOutputEndpoints)
                    {
                        _endpointTableManager.DeleteEndpoint(processName, key.Key);
                    }

                    IProcess old;
                    if (!table.TryRemove(processName, out old))
                    {
                        Console.WriteLine("Unable to remove process on disposal");
                    }
                    var entity = new DynamicTableEntity(instanceName, processName);
                    entity.ETag = "*";
                    TableOperation deleteOperation = TableOperation.Delete(entity);
                    _processTable.Execute(deleteOperation);
                });
            }

            // INITIALIZE
            if ((ProcessBase)process != null)
            {
                ((ProcessBase)process).ProcessName = processName;
                ((ProcessBase)process).ClientLibrary = this;
            }

            var parametersBlob = container.GetBlockBlobReference(processDefinition + "/" + processName);
            Stream parametersStream = parametersBlob.OpenRead();
            byte[] parametersBytes = parametersStream.ReadByteArray();
            string parameterString = Encoding.UTF8.GetString(parametersBytes);
            parametersStream.Close();

            var par = SerializationHelper.DeserializeObject(parameterString);
            process.Initialize(par);

            return process;
        }

        /// <summary>
        /// Load all processes for the given instance name.
        /// </summary>
        /// <param name="thisInstanceName"></param>
        /// <returns></returns>
        public ConcurrentDictionary<string, IProcess> LoadAllProcesses(string thisInstanceName)
        {
            ConcurrentDictionary<string, IProcess> result = new ConcurrentDictionary<string, IProcess>();
            var rows = ProcessTable.GetAllRowsForInstance(_processTable, thisInstanceName);

            foreach (var row in rows)
            {
                if (row.ProcessName == "") continue;
                LoadProcess(row.ProcessName, row.ProcessDefinition, row.ProcessParameter, thisInstanceName, result);
            }

            return result;
        }

        /// <summary>
        /// Add connection info to metadata table
        /// </summary>
        /// <param name="fromProcessName"></param>
        /// <param name="fromEndpoint"></param>
        /// <param name="toProcessName"></param>
        /// <param name="toEndpoint"></param>
        public void AddConnectionInfo(string fromProcessName, string fromEndpoint, string toProcessName, string toEndpoint)
        {
            _connectionTableManager.AddConnection(fromProcessName, fromEndpoint, toProcessName, toEndpoint);
        }


        /// <summary>
        /// Delete connection info from metadata table
        /// </summary>
        /// <param name="fromProcessName"></param>
        /// <param name="fromEndpoint"></param>
        /// <param name="toProcessName"></param>
        /// <param name="toEndpoint"></param>
        public void DeleteConnectionInfo(string fromProcessName, string fromEndpoint, string toProcessName, string toEndpoint)
        {
            _connectionTableManager.DeleteConnection(fromProcessName, fromEndpoint, toProcessName, toEndpoint);
        }

        /// <summary>
        /// Delete connection info from metadata table
        /// </summary>
        /// <param name="connInfo">Connection info as a struct</param>
        public void DeleteConnectionInfo(ConnectionInfo connInfo)
        {
            _connectionTableManager.DeleteConnection(connInfo.FromProcess, connInfo.FromEndpoint, connInfo.ToProcess, connInfo.ToEndpoint);
        }

        /// <summary>
        /// Connect one CRA process to another, via pre-defined endpoints. We contact the "from" process
        /// to initiate the creation of the link.
        /// </summary>
        /// <param name="fromProcessName">Name of the process from which connection is being made</param>
        /// <param name="fromEndpoint">Name of the endpoint on the fromProcess, from which connection is being made</param>
        /// <param name="toProcessName">Name of the process to which connection is being made</param>
        /// <param name="toEndpoint">Name of the endpoint on the toProcess, to which connection is being made</param>
        /// <returns>Status of the Connect operation</returns>
        public CRAErrorCode Connect(string fromProcessName, string fromEndpoint, string toProcessName, string toEndpoint)
        {
            return Connect(fromProcessName, fromEndpoint, toProcessName, toEndpoint, ConnectionInitiator.FromSide);
        }

        /// <summary>
        /// Connect one CRA process to another, via pre-defined endpoints. We contact the "from" process
        /// to initiate the creation of the link.
        /// </summary>
        /// <param name="fromProcessName">Name of the process from which connection is being made</param>
        /// <param name="fromEndpoint">Name of the endpoint on the fromProcess, from which connection is being made</param>
        /// <param name="toProcessName">Name of the process to which connection is being made</param>
        /// <param name="toEndpoint">Name of the endpoint on the toProcess, to which connection is being made</param>
        /// <param name="direction">Which process initiates the connection</param>
        /// <returns>Status of the Connect operation</returns>
        public CRAErrorCode Connect(string fromProcessName, string fromEndpoint, string toProcessName, string toEndpoint, ConnectionInitiator direction)
        {
            // Tell from process to establish connection
            // Send request to CRA instance

            // Get instance for source process
            var _row = direction == ConnectionInitiator.FromSide ?
                                        ProcessTable.GetRowForProcess(_processTable, fromProcessName) :
                                        ProcessTable.GetRowForProcess(_processTable, toProcessName);


            // Check that process and endpoints are valid and existing
            if (!_processTableManager.ExistsProcess(fromProcessName) || !_processTableManager.ExistsProcess(toProcessName))
            {
                return CRAErrorCode.ProcessNotFound;
            }

            // Make the connection information stable
            _connectionTableManager.AddConnection(fromProcessName, fromEndpoint, toProcessName, toEndpoint);

            // We now try best-effort to tell the CRA instance of this connection
            CRAErrorCode result = CRAErrorCode.Success;

            if (_localWorker != null)
            {
                if (_localWorker.InstanceName == _row.InstanceName)
                {
                    return _localWorker.Connect_InitiatorSide(fromProcessName, fromEndpoint,
                            toProcessName, toEndpoint, direction == ConnectionInitiator.ToSide);
                }
            }

            // Send request to CRA instance
            try
            {
                // Get address and port for instance, using row with process = ""
                var row = ProcessTable.GetRowForInstance(_processTable, _row.InstanceName);

                // Get a stream connection from the pool if available
                NetworkStream stream;
                if (!TryGetSenderStreamFromPool(row.Address, row.Port.ToString(), out stream))
                {
                    TcpClient client = new TcpClient(row.Address, row.Port);
                    client.NoDelay = true;

                    stream = client.GetStream();
                }

                if (direction == ConnectionInitiator.FromSide)
                    stream.WriteInt32((int)CRATaskMessageType.CONNECT_PROCESS_INITIATOR);
                else
                    stream.WriteInt32((int)CRATaskMessageType.CONNECT_PROCESS_INITIATOR_REVERSE);

                stream.WriteByteArray(Encoding.UTF8.GetBytes(fromProcessName));
                stream.WriteByteArray(Encoding.UTF8.GetBytes(fromEndpoint));
                stream.WriteByteArray(Encoding.UTF8.GetBytes(toProcessName));
                stream.WriteByteArray(Encoding.UTF8.GetBytes(toEndpoint));

                result = (CRAErrorCode)stream.ReadInt32();
                if (result != 0)
                {
                    Console.WriteLine("Connection was logically established. However, the client received an error code from the connection-initiating CRA instance: " + result);
                }
                else
                {
                    // Add/Return a stream connection to the pool
                    TryAddSenderStreamToPool(row.Address, row.Port.ToString(), stream);
                }
            }
            catch
            {
                Console.WriteLine("The connection-initiating CRA instance appears to be down or could not be found. Restart it and this connection will be completed automatically");
            }
            return (CRAErrorCode)result;
        }


        public string GetDefaultInstanceName()
        {
            return _processTableManager.GetRowForDefaultInstance().InstanceName;
        }

        /// <summary>
        /// Get a list of all output endpoint names for a given process
        /// </summary>
        /// <param name="processName"></param>
        /// <returns></returns>
        public IEnumerable<string> GetOutputEndpoints(string processName)
        {
            return _endpointTableManager.GetOutputEndpoints(processName);
        }

        /// <summary>
        /// Get a list of all input endpoint names for a given process
        /// </summary>
        /// <param name="processName"></param>
        /// <returns></returns>
        public IEnumerable<string> GetInputEndpoints(string processName)
        {
            return _endpointTableManager.GetInputEndpoints(processName);
        }

        /// <summary>
        /// Get all outgoing connection from a given process
        /// </summary>
        /// <param name="processName"></param>
        /// <returns></returns>
        public IEnumerable<ConnectionInfo> GetConnectionsFromProcess(string processName)
        {
            return _connectionTableManager.GetConnectionsFromProcess(processName);
        }

        /// <summary>
        /// Get all incoming connections to a given process
        /// </summary>
        /// <param name="processName"></param>
        /// <returns></returns>
        public IEnumerable<ConnectionInfo> GetConnectionsToProcess(string processName)
        {
            return _connectionTableManager.GetConnectionsToProcess(processName);
        }


        /// <summary>
        /// Gets a list of all processes registered with CRA
        /// </summary>
        /// <returns></returns>
        public IEnumerable<string> ProcessNames
        {
            get
            {
                return _processTableManager.GetProcessNames();
            }
        }

        /// <summary>
        /// Gets a list of all process definitions registered with CRA
        /// </summary>
        /// <returns></returns>
        public IEnumerable<string> ProcessDefinitions
        {
            get
            {
                return _processTableManager.GetProcessDefinitions();
            }
        }

        /// <summary>
        /// Gets a list of all registered CRA instances
        /// </summary>
        /// <returns></returns>
        public IEnumerable<string> InstanceNames
        {
            get
            {
                return _processTableManager.GetInstanceNames();
            }
        }

        private CloudTable CreateTableIfNotExists(string tableName)
        {
            CloudTable table = _tableClient.GetTableReference(tableName);
            try
            {
                table.CreateIfNotExists();
            }
            catch { }

            return table;
        }

        /// <summary>
        /// Disconnect a CRA connection
        /// </summary>
        /// <param name="fromProcessName"></param>
        /// <param name="fromProcessOutput"></param>
        /// <param name="toProcessName"></param>
        /// <param name="toProcessInput"></param>
        public void Disconnect(string fromProcessName, string fromProcessOutput, string toProcessName, string toProcessInput)
        {
            _connectionTableManager.DeleteConnection(fromProcessName, fromProcessOutput, toProcessName, toProcessInput);
        }

    }
}
