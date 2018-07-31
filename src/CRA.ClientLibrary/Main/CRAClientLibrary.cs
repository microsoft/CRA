using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

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

        CloudTable _vertexTable;
        CloudTable _connectionTable;

        internal VertexTableManager _vertexTableManager;
        internal ShardedVertexTableManager _shardedVertexTableManager;
        EndpointTableManager _endpointTableManager;
        ConnectionTableManager _connectionTableManager;

        Type aquaType = typeof(Aqua.TypeSystem.ConstructorInfo);

        public ISecureStreamConnectionDescriptor SecureStreamConnectionDescriptor = new DummySecureStreamConnectionDescriptor();


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
                _storageConnectionString = null;
#if !DOTNETCORE
                _storageConnectionString = ConfigurationManager.AppSettings.Get("AZURE_STORAGE_CONN_STRING");
#endif
                if (_storageConnectionString == null)
                {
                    _storageConnectionString = Environment.GetEnvironmentVariable("AZURE_STORAGE_CONN_STRING");
                }
                if (_storageConnectionString == null)
                {
                    throw new InvalidOperationException("Azure storage connection string not found. Use appSettings in your app.config to provide this using the key AZURE_STORAGE_CONN_STRING, or use the environment variable AZURE_STORAGE_CONN_STRING.");
                }
            }
            else
                _storageConnectionString = storageConnectionString;

            _storageAccount = CloudStorageAccount.Parse(_storageConnectionString);

            _blobClient = _storageAccount.CreateCloudBlobClient();
            _tableClient = _storageAccount.CreateCloudTableClient();

            _vertexTableManager = new VertexTableManager(_storageConnectionString);
            _shardedVertexTableManager = new ShardedVertexTableManager(_storageConnectionString);

            _endpointTableManager = new EndpointTableManager(_storageConnectionString);
            _connectionTableManager = new ConnectionTableManager(_storageConnectionString);

            _vertexTable = CreateTableIfNotExists("cravertextable");
            _connectionTable = CreateTableIfNotExists("craconnectiontable");
        }

        /// <summary>
        /// Define secure stream connections
        /// </summary>
        /// <param name="descriptor"></param>
        public void DefineSecureStreamConnection(ISecureStreamConnectionDescriptor descriptor)
        {
            this.SecureStreamConnectionDescriptor = descriptor;
        }

        /// <summary>
        /// Define a vertex type and register with CRA.
        /// </summary>
        /// <param name="vertexDefinition">Name of the vertex type</param>
        /// <param name="creator">Lambda that describes how to instantiate the vertex, taking in an object as parameter</param>
        public CRAErrorCode DefineVertex(string vertexDefinition, Expression<Func<IVertex>> creator)
        {
            CloudBlobContainer container = _blobClient.GetContainerReference("cra");
            container.CreateIfNotExistsAsync().Wait();
            var blockBlob = container.GetBlockBlobReference(vertexDefinition + "/binaries");
            CloudBlobStream blobStream = blockBlob.OpenWriteAsync().GetAwaiter().GetResult();
            AssemblyUtils.WriteAssembliesToStream(blobStream);
            blobStream.Close();

            // Add metadata
            var newRow = new VertexTable("", vertexDefinition, vertexDefinition, "", 0, creator, null, true);
            TableOperation insertOperation = TableOperation.InsertOrReplace(newRow);
            _vertexTable.ExecuteAsync(insertOperation).Wait();

            return CRAErrorCode.Success;
        }

        /// <summary>
        /// Make this vertex the current "active".
        /// </summary>
        /// <param name="vertexName"></param>
        /// <param name="instanceName"></param>
        public void ActivateVertex(string vertexName, string instanceName)
        {
            _vertexTableManager.ActivateVertexOnInstance(vertexName, instanceName);
        }

        /// <summary>
        /// Make this vertex the current "inactive".
        /// </summary>
        /// <param name="vertexName"></param>
        /// <param name="instanceName"></param>
        public void DeactivateVertex(string vertexName, string instanceName)
        {
            _vertexTableManager.DeactivateVertexOnInstance(vertexName, instanceName);
        }

        /// <summary>
        /// Make this vertex the current "active" on local worker.
        /// </summary>
        /// <param name="vertexName"></param>
        /// <param name="instanceName"></param>
        public void ActivateVertex(string vertexName)
        {
            if (_localWorker != null)
                _vertexTableManager.ActivateVertexOnInstance(vertexName, _localWorker.InstanceName);
            else
                throw new Exception("No local worker found to activate vertex on");
        }

        /// <summary>
        /// Resets the cluster and deletes all knowledge of any CRA instances
        /// </summary>
        public void Reset()
        {
            _connectionTable.DeleteIfExistsAsync().Wait();
            _vertexTable.DeleteIfExistsAsync().Wait();
            _endpointTableManager.DeleteTable();
        }

        /// <summary>
        /// Delete contents of a cloud table
        /// </summary>
        /// <param name="_table"></param>
        private static void DeleteContents(CloudTable table)
        {
            Action<IEnumerable<DynamicTableEntity>> vertexor = entities =>
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
                        table.ExecuteBatchAsync(batch).Wait();
                        batches[entity.PartitionKey] = new TableBatchOperation();
                    }
                }

                foreach (var batch in batches.Values)
                {
                    if (batch.Count > 0)
                    {
                        table.ExecuteBatchAsync(batch).Wait();
                    }
                }
            };

            VertexEntities(table, vertexor);
        }

        /// <summary>
        /// Vertex all entities in a cloud table using the given vertexor lambda.
        /// </summary>
        /// <param name="table"></param>
        /// <param name="vertexor"></param>
        private static void VertexEntities(CloudTable table, Action<IEnumerable<DynamicTableEntity>> vertexor)
        {
            vertexor(table.ExecuteQuery(new TableQuery<DynamicTableEntity>()));
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
        /// Instantiate a vertex on a CRA instance.
        /// </summary>
        /// <param name="instanceName">Name of the CRA instance on which vertex is instantiated</param>
        /// <param name="vertexName">Name of the vertex (particular instance)</param>
        /// <param name="vertexDefinition">Definition of the vertex (type)</param>
        /// <param name="vertexParameter">Parameters to be passed to the vertex in its constructor (serializable object)</param>
        /// <returns>Status of the command</returns>
        public CRAErrorCode InstantiateVertex(string instanceName, string vertexName, string vertexDefinition, object vertexParameter)
        {
            return InstantiateVertex(instanceName, vertexName, vertexDefinition, vertexParameter, false);
        }

        internal CRAErrorCode InstantiateVertex(string instanceName, string vertexName, string vertexDefinition, object vertexParameter, bool sharded)
        { 
            var procDefRow = VertexTable.GetRowForVertexDefinition(_vertexTable, vertexDefinition);

            string blobName = vertexName + "-" + instanceName;

            // Serialize and write the vertex parameters to a blob
            CloudBlobContainer container = _blobClient.GetContainerReference("cra");
            container.CreateIfNotExistsAsync().Wait();
            var blockBlob = container.GetBlockBlobReference(vertexDefinition + "/" + blobName);
            CloudBlobStream blobStream = blockBlob.OpenWriteAsync().GetAwaiter().GetResult();
            byte[] parameterBytes = Encoding.UTF8.GetBytes(
                        SerializationHelper.SerializeObject(vertexParameter));
            blobStream.WriteByteArray(parameterBytes);
            blobStream.Close();

            // Add metadata
            var newRow = new VertexTable(instanceName, vertexName, vertexDefinition, "", 0,
                procDefRow.VertexCreateAction,
                blobName,
                false, sharded);
            TableOperation insertOperation = TableOperation.InsertOrReplace(newRow);
            _vertexTable.ExecuteAsync(insertOperation).Wait();

            CRAErrorCode result = CRAErrorCode.Success;

            // Send request to CRA instance
            VertexTable instanceRow;
            try
            {
                instanceRow = VertexTable.GetRowForInstance(_vertexTable, instanceName);

                // Get a stream connection from the pool if available
                Stream stream;
                if (!TryGetSenderStreamFromPool(instanceRow.Address, instanceRow.Port.ToString(), out stream))
                {
                    TcpClient client = new TcpClient(instanceRow.Address, instanceRow.Port);
                    client.NoDelay = true;

                    stream = client.GetStream();
                    if (SecureStreamConnectionDescriptor != null)
                        stream = SecureStreamConnectionDescriptor.CreateSecureClient(stream, instanceName);
                }

                stream.WriteInt32((int)CRATaskMessageType.LOAD_VERTEX);
                stream.WriteByteArray(Encoding.UTF8.GetBytes(vertexName));
                stream.WriteByteArray(Encoding.UTF8.GetBytes(vertexDefinition));
                stream.WriteByteArray(Encoding.UTF8.GetBytes(newRow.VertexParameter));
                result = (CRAErrorCode)stream.ReadInt32();
                if (result != 0)
                {
                    Console.WriteLine("Vertex was logically loaded. However, we received an error code from the hosting CRA instance: " + result);
                }

                // Add/Return stream connection to the pool
                TryAddSenderStreamToPool(instanceRow.Address, instanceRow.Port.ToString(), stream);
            }
            catch
            {
                Console.WriteLine("The CRA instance appears to be down. Restart it and this vertex will be instantiated automatically");
            }
            return result;
        }

        /// <summary>
        /// Register caller as a vertex with given name, dummy temp instance
        /// </summary>
        /// <param name="vertexName"></param>
        /// <returns></returns>
        public DetachedVertex RegisterAsVertex(string vertexName)
        {
            return new DetachedVertex(vertexName, "", this);
        }

        /// <summary>
        /// Register caller as a vertex with given name, given CRA instance name
        /// </summary>
        /// <param name="vertexName"></param>
        /// <param name="instanceName"></param>
        /// <returns></returns>
        public DetachedVertex RegisterAsVertex(string vertexName, string instanceName)
        {
            return new DetachedVertex(vertexName, instanceName, this);
        }

        /// <summary>
        /// Register CRA instance name
        /// </summary>
        /// <param name="instanceName"></param>
        /// <param name="address"></param>
        /// <param name="port"></param>
        public void RegisterInstance(string instanceName, string address, int port)
        {
            _vertexTableManager.RegisterInstance(instanceName, address, port);
        }

        /// <summary>
        /// Delete CRA instance name
        /// </summary>
        /// <param name="instanceName"></param>
        public void DeleteInstance(string instanceName)
        {
            _vertexTableManager.DeleteInstance(instanceName);
        }

        /// <summary>
        /// Delete vertex with given name
        /// </summary>
        /// <param name="vertexName"></param>
        /// <param name="instanceName"></param>
        public void DeleteVertex(string vertexName)
        {
            foreach (var endpt in GetInputEndpoints(vertexName))
            {
                DeleteEndpoint(vertexName, endpt);
            }
            foreach (var endpt in GetOutputEndpoints(vertexName))
            {
                DeleteEndpoint(vertexName, endpt);
            }

            foreach (var conn in GetConnectionsFromVertex(vertexName))
            {
                DeleteConnectionInfo(conn);
            }
            foreach (var conn in GetConnectionsToVertex(vertexName))
            {
                DeleteConnectionInfo(conn);
            }

            var query = new TableQuery<VertexTable>()
                   .Where(TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, vertexName));

            foreach (var item in _vertexTable.ExecuteQuery(query))
            {
                var oper = TableOperation.Delete(item);
                _vertexTable.ExecuteAsync(oper).Wait();
            }
        }

        /// <summary>
        /// Delete vertex definition with given name
        /// </summary>
        /// <param name="vertexDefinition"></param>
        public void DeleteVertexDefinition(string vertexDefinition)
        {
            var entity = new DynamicTableEntity("", vertexDefinition);
            entity.ETag = "*";
            TableOperation deleteOperation = TableOperation.Delete(entity);
            _vertexTable.ExecuteAsync(deleteOperation).Wait();
            CloudBlobContainer container = _blobClient.GetContainerReference("cra");
            container.CreateIfNotExistsAsync().Wait();
            var blockBlob = container.GetBlockBlobReference(vertexDefinition + "/binaries");
            blockBlob.DeleteIfExistsAsync().Wait();
        }


        /// <summary>
        /// Add endpoint to the appropriate CRA metadata table
        /// </summary>
        /// <param name="vertexName"></param>
        /// <param name="endpointName"></param>
        /// <param name="isInput"></param>
        /// <param name="isAsync"></param>
        public void AddEndpoint(string vertexName, string endpointName, bool isInput, bool isAsync)
        {
            _endpointTableManager.AddEndpoint(vertexName, endpointName, isInput, isAsync);
        }

        /// <summary>
        /// Delete endpoint
        /// </summary>
        /// <param name="vertexName"></param>
        /// <param name="endpointName"></param>
        public void DeleteEndpoint(string vertexName, string endpointName)
        {
            _endpointTableManager.DeleteEndpoint(vertexName, endpointName);
        }

        /// <summary>
        /// Load a vertex on the local instance
        /// </summary>
        /// <param name="vertexName"></param>
        /// <param name="vertexDefinition"></param>
        /// <param name="vertexParameter"></param>
        /// <param name="instanceName"></param>
        /// <param name="table"></param>
        /// <returns></returns>
        public async Task<IVertex> LoadVertexAsync(string vertexName, string vertexDefinition, string vertexParameter, string instanceName, ConcurrentDictionary<string, IVertex> table)
        {
            // Deactivate vertex
            _vertexTableManager.DeactivateVertexOnInstance(vertexName, instanceName);

            CloudBlobContainer container = _blobClient.GetContainerReference("cra");
            container.CreateIfNotExistsAsync().Wait();
            var blockBlob = container.GetBlockBlobReference(vertexDefinition + "/binaries");
            Stream blobStream = blockBlob.OpenReadAsync().GetAwaiter().GetResult();
            AssemblyUtils.LoadAssembliesFromStream(blobStream);
            blobStream.Close();

            var row = VertexTable.GetRowForVertexDefinition(_vertexTable, vertexDefinition);

            // CREATE THE VERTEX
            var vertex = row.GetVertexCreateAction()();

            // INITIALIZE
            if ((VertexBase)vertex != null)
            {
                ((VertexBase)vertex).VertexName = vertexName;
                ((VertexBase)vertex).ClientLibrary = this;
            }

            // LATCH CALLBACKS TO POPULATE ENDPOINT TABLE
            vertex.OnAddInputEndpoint((name, endpt) => _endpointTableManager.AddEndpoint(vertexName, name, true, false));
            vertex.OnAddOutputEndpoint((name, endpt) => _endpointTableManager.AddEndpoint(vertexName, name, false, false));
            vertex.OnAddAsyncInputEndpoint((name, endpt) => _endpointTableManager.AddEndpoint(vertexName, name, true, true));
            vertex.OnAddAsyncOutputEndpoint((name, endpt) => _endpointTableManager.AddEndpoint(vertexName, name, false, true));

            //ADD TO TABLE
            if (table != null)
            {
                table.AddOrUpdate(vertexName, vertex, (procName, oldProc) => { oldProc.Dispose(); return vertex; });

                vertex.OnDispose(() =>
                {
                    // Delete all endpoints of the vertex
                    foreach (var key in vertex.InputEndpoints)
                    {
                        _endpointTableManager.DeleteEndpoint(vertexName, key.Key);
                    }
                    foreach (var key in vertex.AsyncInputEndpoints)
                    {
                        _endpointTableManager.DeleteEndpoint(vertexName, key.Key);
                    }
                    foreach (var key in vertex.OutputEndpoints)
                    {
                        _endpointTableManager.DeleteEndpoint(vertexName, key.Key);
                    }
                    foreach (var key in vertex.AsyncOutputEndpoints)
                    {
                        _endpointTableManager.DeleteEndpoint(vertexName, key.Key);
                    }

                    IVertex old;
                    if (!table.TryRemove(vertexName, out old))
                    {
                        Console.WriteLine("Unable to remove vertex on disposal");
                    }
                    var entity = new DynamicTableEntity(instanceName, vertexName);
                    entity.ETag = "*";
                    TableOperation deleteOperation = TableOperation.Delete(entity);
                    _vertexTable.ExecuteAsync(deleteOperation).Wait();
                });
            }


            string blobName = vertexName + "-" + instanceName;

            var parametersBlob = container.GetBlockBlobReference(vertexDefinition + "/" + blobName);
            Stream parametersStream = parametersBlob.OpenReadAsync().GetAwaiter().GetResult();
            byte[] parametersBytes = parametersStream.ReadByteArray();
            string parameterString = Encoding.UTF8.GetString(parametersBytes);
            parametersStream.Close();

            var par = SerializationHelper.DeserializeObject(parameterString);
            vertex.Initialize(par);
            await vertex.InitializeAsync(par);

            // Activate vertex
            ActivateVertex(vertexName, instanceName);

            return vertex;
        }

        /// <summary>
        /// Load all vertices for the given instance name, returns only when all
        /// vertices have been initialized and activated.
        /// </summary>
        /// <param name="thisInstanceName"></param>
        /// <returns></returns>
        public ConcurrentDictionary<string, IVertex> LoadAllVertices(string thisInstanceName)
        {
            ConcurrentDictionary<string, IVertex> result = new ConcurrentDictionary<string, IVertex>();
            var rows = VertexTable.GetAllRowsForInstance(_vertexTable, thisInstanceName);

            List<Task> t = new List<Task>();
            foreach (var row in rows)
            {
                if (row.VertexName == "") continue;
                t.Add(LoadVertexAsync(row.VertexName, row.VertexDefinition, row.VertexParameter, thisInstanceName, result));
            }
            Task.WaitAll(t.ToArray());

            return result;
        }

        /// <summary>
        /// Add connection info to metadata table
        /// </summary>
        /// <param name="fromVertexName"></param>
        /// <param name="fromEndpoint"></param>
        /// <param name="toVertexName"></param>
        /// <param name="toEndpoint"></param>
        public void AddConnectionInfo(string fromVertexName, string fromEndpoint, string toVertexName, string toEndpoint)
        {
            _connectionTableManager.AddConnection(fromVertexName, fromEndpoint, toVertexName, toEndpoint);
        }


        /// <summary>
        /// Delete connection info from metadata table
        /// </summary>
        /// <param name="fromVertexName"></param>
        /// <param name="fromEndpoint"></param>
        /// <param name="toVertexName"></param>
        /// <param name="toEndpoint"></param>
        public void DeleteConnectionInfo(string fromVertexName, string fromEndpoint, string toVertexName, string toEndpoint)
        {
            _connectionTableManager.DeleteConnection(fromVertexName, fromEndpoint, toVertexName, toEndpoint);
        }

        /// <summary>
        /// Delete connection info from metadata table
        /// </summary>
        /// <param name="connInfo">Connection info as a struct</param>
        public void DeleteConnectionInfo(ConnectionInfo connInfo)
        {
            _connectionTableManager.DeleteConnection(connInfo.FromVertex, connInfo.FromEndpoint, connInfo.ToVertex, connInfo.ToEndpoint);
        }

        /// <summary>
        /// Connect one CRA vertex to another, via pre-defined endpoints. We contact the "from" vertex
        /// to initiate the creation of the link.
        /// </summary>
        /// <param name="fromVertexName">Name of the vertex from which connection is being made</param>
        /// <param name="fromEndpoint">Name of the endpoint on the fromVertex, from which connection is being made</param>
        /// <param name="toVertexName">Name of the vertex to which connection is being made</param>
        /// <param name="toEndpoint">Name of the endpoint on the toVertex, to which connection is being made</param>
        /// <returns>Status of the Connect operation</returns>
        public CRAErrorCode Connect(string fromVertexName, string fromEndpoint, string toVertexName, string toEndpoint)
        {
            return Connect(fromVertexName, fromEndpoint, toVertexName, toEndpoint, ConnectionInitiator.FromSide);
        }

        /// <summary>
        /// Connect one CRA vertex to another, via pre-defined endpoints. We contact the "from" vertex
        /// to initiate the creation of the link.
        /// </summary>
        /// <param name="fromVertexName">Name of the vertex from which connection is being made</param>
        /// <param name="fromEndpoint">Name of the endpoint on the fromVertex, from which connection is being made</param>
        /// <param name="toVertexName">Name of the vertex to which connection is being made</param>
        /// <param name="toEndpoint">Name of the endpoint on the toVertex, to which connection is being made</param>
        /// <param name="direction">Which vertex initiates the connection</param>
        /// <returns>Status of the Connect operation</returns>
        public CRAErrorCode Connect(string fromVertexName, string fromEndpoint, string toVertexName, string toEndpoint, ConnectionInitiator direction)
        {
            // Tell from vertex to establish connection
            // Send request to CRA instance

            // Check that vertex and endpoints are valid and existing
            if (!_vertexTableManager.ExistsVertex(fromVertexName) || !_vertexTableManager.ExistsVertex(toVertexName))
            {
                // Check for sharded vertices
                List<int> fromVertexShards, toVertexShards;

                if (!_vertexTableManager.ExistsShardedVertex(fromVertexName, out fromVertexShards))
                    return CRAErrorCode.VertexNotFound;

                if (!_vertexTableManager.ExistsShardedVertex(toVertexName, out toVertexShards))
                    return CRAErrorCode.VertexNotFound;

                return ConnectSharded(fromVertexName, fromVertexShards, fromEndpoint, toVertexName, toVertexShards, toEndpoint, direction);
            }

            // Make the connection information stable
            _connectionTableManager.AddConnection(fromVertexName, fromEndpoint, toVertexName, toEndpoint);

            // We now try best-effort to tell the CRA instance of this connection
            CRAErrorCode result = CRAErrorCode.Success;

            VertexTable _row;
            try
            {
                // Get instance for source vertex
                _row = direction == ConnectionInitiator.FromSide ?
                                        VertexTable.GetRowForVertex(_vertexTable, fromVertexName) :
                                        VertexTable.GetRowForVertex(_vertexTable, toVertexName);
            }
            catch
            {
                Console.WriteLine("Unable to find active instance with vertex. On vertex activation, the connection should be completed automatically.");
                return result;
            }

            try
            {
                if (_localWorker != null)
                {
                    if (_localWorker.InstanceName == _row.InstanceName)
                    {
                        return _localWorker.Connect_InitiatorSide(fromVertexName, fromEndpoint,
                                toVertexName, toEndpoint, direction == ConnectionInitiator.ToSide);
                    }
                }


                // Send request to CRA instance
                TcpClient client = null;
                // Get address and port for instance, using row with vertex = ""
                var row = VertexTable.GetRowForInstance(_vertexTable, _row.InstanceName);

                // Get a stream connection from the pool if available
                Stream stream;
                if (!TryGetSenderStreamFromPool(row.Address, row.Port.ToString(), out stream))
                {
                    client = new TcpClient(row.Address, row.Port);
                    client.NoDelay = true;

                    stream = client.GetStream();

                    if (SecureStreamConnectionDescriptor != null)
                        stream = SecureStreamConnectionDescriptor.CreateSecureClient(stream, _row.InstanceName);
                }

                if (direction == ConnectionInitiator.FromSide)
                    stream.WriteInt32((int)CRATaskMessageType.CONNECT_VERTEX_INITIATOR);
                else
                    stream.WriteInt32((int)CRATaskMessageType.CONNECT_VERTEX_INITIATOR_REVERSE);

                stream.WriteByteArray(Encoding.UTF8.GetBytes(fromVertexName));
                stream.WriteByteArray(Encoding.UTF8.GetBytes(fromEndpoint));
                stream.WriteByteArray(Encoding.UTF8.GetBytes(toVertexName));
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
            return result;
        }

        private CRAErrorCode ConnectSharded(string fromVertexName, List<int> fromVertexShards, string fromEndpoint, string toVertexName, List<int> toVertexShards, string toEndpoint, ConnectionInitiator direction)
        {
            var fromVertexNames = fromVertexShards.Select(e => fromVertexName + "$" + e).ToArray();
            var toVertexNames = toVertexShards.Select(e => toVertexName + "$" + e).ToArray();
            var fromEndpoints = toVertexShards.Select(e => fromEndpoint + "$" + e).ToArray();
            var toEndpoints = fromVertexShards.Select(e => toEndpoint + "$" + e).ToArray();

            return
                ConnectShardedVerticesWithFullMesh(fromVertexNames, fromEndpoints, toVertexNames, toEndpoints, direction);
        }

        public string GetDefaultInstanceName()
        {
            return _vertexTableManager.GetRowForDefaultInstance().InstanceName;
        }

        /// <summary>
        /// Get a list of all output endpoint names for a given vertex
        /// </summary>
        /// <param name="vertexName"></param>
        /// <returns></returns>
        public IEnumerable<string> GetOutputEndpoints(string vertexName)
        {
            return _endpointTableManager.GetOutputEndpoints(vertexName);
        }

        /// <summary>
        /// Get a list of all input endpoint names for a given vertex
        /// </summary>
        /// <param name="vertexName"></param>
        /// <returns></returns>
        public IEnumerable<string> GetInputEndpoints(string vertexName)
        {
            return _endpointTableManager.GetInputEndpoints(vertexName);
        }

        /// <summary>
        /// Get all outgoing connection from a given vertex
        /// </summary>
        /// <param name="vertexName"></param>
        /// <returns></returns>
        public IEnumerable<ConnectionInfo> GetConnectionsFromVertex(string vertexName)
        {
            return _connectionTableManager.GetConnectionsFromVertex(vertexName);
        }

        /// <summary>
        /// Get all incoming connections to a given vertex
        /// </summary>
        /// <param name="vertexName"></param>
        /// <returns></returns>
        public IEnumerable<ConnectionInfo> GetConnectionsToVertex(string vertexName)
        {
            return _connectionTableManager.GetConnectionsToVertex(vertexName);
        }


        /// <summary>
        /// Gets a list of all vertices registered with CRA
        /// </summary>
        /// <returns></returns>
        public IEnumerable<string> VertexNames
        {
            get
            {
                return _vertexTableManager.GetVertexNames();
            }
        }

        /// <summary>
        /// Gets a list of all vertex definitions registered with CRA
        /// </summary>
        /// <returns></returns>
        public IEnumerable<string> VertexDefinitions
        {
            get
            {
                return _vertexTableManager.GetVertexDefinitions();
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
                return _vertexTableManager.GetInstanceNames();
            }
        }

        private CloudTable CreateTableIfNotExists(string tableName)
        {
            CloudTable table = _tableClient.GetTableReference(tableName);
            try
            {
                table.CreateIfNotExistsAsync().Wait();
            }
            catch { }

            return table;
        }

        /// <summary>
        /// Disconnect a CRA connection
        /// </summary>
        /// <param name="fromVertexName"></param>
        /// <param name="fromVertexOutput"></param>
        /// <param name="toVertexName"></param>
        /// <param name="toVertexInput"></param>
        public void Disconnect(string fromVertexName, string fromVertexOutput, string toVertexName, string toVertexInput)
        {
            _connectionTableManager.DeleteConnection(fromVertexName, fromVertexOutput, toVertexName, toVertexInput);
        }

        /// <summary>
        /// Terminal local worker process.
        /// </summary>
        /// <param name="killMessage">Message to display on kill</param>
        public void KillLocalWorker(string killMessage)
        {
            if (_localWorker != null)
            {
                _localWorker.Kill(killMessage);
            }
        }
    }
}
