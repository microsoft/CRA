using CRA.ClientLibrary.DataProvider;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
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
        // Azure storage clients
        internal CRAWorker _localWorker;
        internal IBlobStorageProvider _blobStorage;
        internal VertexTableManager _vertexManager;
        internal ShardedVertexTableManager _shardedVertexTableManager;
        internal EndpointTableManager _endpointTableManager;
        internal ConnectionTableManager _connectionTableManager;

        Type aquaType = typeof(Aqua.TypeSystem.ConstructorInfo);

        public ISecureStreamConnectionDescriptor SecureStreamConnectionDescriptor = new DummySecureStreamConnectionDescriptor();

        private Dictionary<string, IVertex> _verticesToSideload = new Dictionary<string, IVertex>();
        private bool _dynamicLoadingEnabled = true;
        private bool _artifactUploading = true;

        public CRAClientLibrary() : this(new AzureProvider.AzureProviderImpl(), null)
        { }

        /// <summary>
        /// Create an instance of the client library for Common Runtime for Applications (CRA)
        /// </summary>
        /// <param name="storageConnectionString">Optional storage account to use for CRA metadata, if
        /// not specified, it will use the appSettings key named StorageConnectionString in app.config</param>
        public CRAClientLibrary(IDataProvider dataProvider) : this(dataProvider, null)
        {
        }

        /// <summary>
        /// Create an instance of the client library for Common Runtime for Applications (CRA)
        /// </summary>
        /// <param name="storageConnectionString">Optional storage account to use for CRA metadata, if
        /// not specified, it will use the appSettings key named StorageConnectionString in app.config</param>
        /// <param name = "localWorker" >Local worker if any</param>
        public CRAClientLibrary(IDataProvider dataProvider, CRAWorker localWorker)
        {
            _localWorker = localWorker;

            _blobStorage = dataProvider.GetBlobStorageProvider();
            _vertexManager = new VertexTableManager(dataProvider);
            _shardedVertexTableManager = new ShardedVertexTableManager(dataProvider);
            _endpointTableManager = new EndpointTableManager(dataProvider);
            _connectionTableManager = new ConnectionTableManager(dataProvider);
            this.DataProvider = dataProvider;
        }

        public IDataProvider DataProvider { get; }

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
        public async Task<CRAErrorCode> DefineVertexAsync(string vertexDefinition, Expression<Func<IVertex>> creator)
        {
            if (_artifactUploading)
            {
                using (var stream = await _blobStorage.GetWriteStream(vertexDefinition + "/binaries"))
                {
                    AssemblyUtils.WriteAssembliesToStream(stream);
                }
            }
            
            var newInfo = VertexInfo.Create(
                "",
                vertexDefinition,
                vertexDefinition,
                "",
                0,
                creator,
                null,
                true,
                false);

            await _vertexManager.VertexInfoProvider.InsertOrReplace(newInfo);

            return CRAErrorCode.Success;
        }

        /// <summary>
        /// Make this vertex the current "active".
        /// </summary>
        /// <param name="vertexName"></param>
        /// <param name="instanceName"></param>
        public async Task ActivateVertexAsync(string vertexName, string instanceName)
            => await _vertexManager.ActivateVertexOnInstance(vertexName, instanceName);

        /// <summary>
        /// Make this vertex the current "inactive".
        /// </summary>
        /// <param name="vertexName"></param>
        /// <param name="instanceName"></param>
        public async Task DeactivateVertexAsync(string vertexName, string instanceName)
            => await _vertexManager.DeactivateVertexOnInstance(vertexName, instanceName);

        /// <summary>
        /// Make this vertex the current "active" on local worker.
        /// </summary>
        /// <param name="vertexName"></param>
        /// <param name="instanceName"></param>
        public async Task ActivateVertexAsync(string vertexName)
        {
            if (_localWorker == null)
            {
                throw new Exception("No local worker found to activate vertex on");
            }

            await _vertexManager.ActivateVertexOnInstance(
                vertexName,
                _localWorker.InstanceName);
        }

        /// <summary>
        /// Resets the cluster and deletes all knowledge of any CRA instances
        /// </summary>
        public async Task ResetAsync()
            => await Task.WhenAll(
                _connectionTableManager.DeleteTable(),
                _vertexManager.DeleteTable(),
                _endpointTableManager.DeleteTable());

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
        public async Task<CRAErrorCode> InstantiateVertexAsync(
            string instanceName,
            string vertexName,
            string vertexDefinition,
            object vertexParameter)
            => await InstantiateVertex(
                instanceName,
                vertexName,
                vertexDefinition,
                vertexParameter,
                false);

        internal async Task<CRAErrorCode> InstantiateVertex(
            string instanceName,
            string vertexName,
            string vertexDefinition,
            object vertexParameter,
            bool sharded)
        { 
            var procDefRow = await _vertexManager.VertexInfoProvider.GetRowForVertexDefinition(vertexDefinition);

            string blobName = vertexName + "-" + instanceName;

            using (var blobStream = await _blobStorage.GetWriteStream(vertexDefinition + "/" + blobName))
            {
                byte[] parameterBytes = Encoding.UTF8.GetBytes(
                            SerializationHelper.SerializeObject(vertexParameter));
                blobStream.WriteByteArray(parameterBytes);
            }

            var newInfo = new VertexInfo(
                instanceName: instanceName,
                address: "",
                port: 0,
                vertexName: vertexName,
                vertexDefinition: vertexDefinition,
                vertexCreateAction: procDefRow.Value.VertexCreateAction,
                vertexParameter: blobName,
                isActive: false,
                isSharded:  sharded);

            await _vertexManager.VertexInfoProvider.InsertOrReplace(newInfo);

            CRAErrorCode result = CRAErrorCode.Success;

            // Send request to CRA instance
            VertexInfo instanceRow;
            try
            {
                instanceRow = (await _vertexManager.GetRowForInstance(instanceName)).Value;

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
                stream.WriteByteArray(Encoding.UTF8.GetBytes(newInfo.VertexParameter));

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
            _vertexManager.RegisterInstance(instanceName, address, port);
        }

        /// <summary>
        /// Delete CRA instance name
        /// </summary>
        /// <param name="instanceName"></param>
        public void DeleteInstance(string instanceName)
        {
            _vertexManager.DeleteInstance(instanceName);
        }

        /// <summary>
        /// Delete vertex with given name
        /// </summary>
        /// <param name="vertexName"></param>
        /// <param name="instanceName"></param>
        public async Task DeleteVertexAsync(string vertexName)
        {
            foreach (var endpt in await GetInputEndpointsAsync(vertexName))
            { DeleteEndpoint(vertexName, endpt); }

            foreach (var endpt in await GetOutputEndpointsAsync(vertexName))
            { DeleteEndpoint(vertexName, endpt); }

            foreach (var conn in await GetConnectionsFromVertexAsync(vertexName))
            { await DeleteConnectionInfoAsync(conn); }

            foreach (var conn in await GetConnectionsToVertexAsync(vertexName))
            { await DeleteConnectionInfoAsync(conn); }

            foreach (var item in await _vertexManager.VertexInfoProvider.GetRowsForVertex(vertexName))
            { await _vertexManager.VertexInfoProvider.DeleteVertexInfo(item); }
        }

        /// <summary>
        /// Delete vertex definition with given name
        /// </summary>
        /// <param name="vertexDefinition"></param>
        public async Task DeleteVertexDefinitionAsync(string vertexDefinition)
        {
            await _vertexManager.DeleteInstanceVertex("", vertexDefinition);

            await _blobStorage.Delete(vertexDefinition + "/binaries");
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
            await _vertexManager.DeactivateVertexOnInstance(vertexName, instanceName);

            var vertex = await CreateVertexAsync(vertexDefinition);
            if (vertex == null)
            {
                if (_verticesToSideload.ContainsKey(vertexName))
                {
                    Debug.WriteLine("Sideloading vertex " + vertexName);
                    vertex = _verticesToSideload[vertexName];
                }
                else
                {
                    throw new InvalidOperationException("Failed to create vertex " + vertexName + ", and no sideloaded vertex with that name was provided.");
                }
            }

            try
            {
                await InitializeVertexAsync(
                    vertexName,
                    vertexDefinition,
                    instanceName,
                    table,
                    vertex);
            }
            catch (Exception e)
            {
                Console.WriteLine("INFO: Unable to initialize vertex " + vertexName + ". Check if runtime is compatible (uploaded vertex and worker should be same .NET runtime). Exception:\n" + e.ToString());
            }

            return vertex;
        }

        public async Task<IVertex> CreateVertexAsync(string vertexDefinition)
        {
            if (_dynamicLoadingEnabled)
            {
                using (Stream blobStream = await _blobStorage.GetReadStream(vertexDefinition + "/binaries"))
                {
                    try
                    {
                        AssemblyUtils.LoadAssembliesFromStream(blobStream);
                    }
                    catch (FileLoadException e)
                    {
                        Debug.WriteLine("Ignoring exception from assembly loading: " + e.Message);
                        Debug.WriteLine("If vertex creation fails, the caller will need to sideload the vertex.");
                    }
                }
            }
            else
            {
                Debug.WriteLine("Dynamic assembly loading is disabled. The caller will need to sideload the vertex.");
            }

            var row = await _vertexManager.VertexInfoProvider.GetRowForVertexDefinition(vertexDefinition);

            // CREATE THE VERTEX
            IVertex vertex = null;
            try
            {
                vertex = row.Value.GetVertexCreateAction()();
            }
            catch (Exception e)
            {
                Debug.WriteLine("Vertex creation failed: " + e.Message);
                Debug.WriteLine("The caller will need to sideload the vertex.");
            }
            return vertex;
        }

        public async Task InitializeVertexAsync(
            string vertexName,
            string vertexDefinition,
            string instanceName,
            ConcurrentDictionary<string, IVertex> table,
            IVertex vertex)
        {
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

                    Task.Run(() =>
                        _vertexManager.DeleteInstanceVertex(
                            instanceName,
                            vertexName));
                });
            }


            string blobName = vertexName + "-" + instanceName;
            string parameterString;

            using(var parametersStream = await _blobStorage.GetReadStream(vertexDefinition + "/" + blobName))
            {
                byte[] parametersBytes = parametersStream.ReadByteArray();
                parameterString = Encoding.UTF8.GetString(parametersBytes);
            }

            var par = SerializationHelper.DeserializeObject(parameterString);
            await vertex.InitializeAsync(par);

            // Activate vertex
            await ActivateVertexAsync(vertexName, instanceName);
        }

        public void SideloadVertex(IVertex vertex, string vertexName)
        {
            _verticesToSideload[vertexName] = vertex;
        }

        public void DisableArtifactUploading()
        {
            _artifactUploading = false;
        }

        public void DisableDynamicLoading()
        {
            _dynamicLoadingEnabled = false;
        }

        /// <summary>
        /// Load all vertices for the given instance name, returns only when all
        /// vertices have been initialized and activated.
        /// </summary>
        /// <param name="thisInstanceName"></param>
        /// <returns></returns>
        public async Task<ConcurrentDictionary<string, IVertex>> LoadAllVerticesAsync(string thisInstanceName)
        {
            ConcurrentDictionary<string, IVertex> result = new ConcurrentDictionary<string, IVertex>();
            var rows = await _vertexManager
                .VertexInfoProvider
                .GetAllRowsForInstance(thisInstanceName);

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
        public async Task AddConnectionInfoAsync(string fromVertexName, string fromEndpoint, string toVertexName, string toEndpoint)
        {
            await _connectionTableManager.AddConnection(fromVertexName, fromEndpoint, toVertexName, toEndpoint);
        }


        /// <summary>
        /// Delete connection info from metadata table
        /// </summary>
        /// <param name="fromVertexName"></param>
        /// <param name="fromEndpoint"></param>
        /// <param name="toVertexName"></param>
        /// <param name="toEndpoint"></param>
        public async Task DeleteConnectionInfoAsync(string fromVertexName, string fromEndpoint, string toVertexName, string toEndpoint)
            => await _connectionTableManager.DeleteConnection(fromVertexName, fromEndpoint, toVertexName, toEndpoint);

        /// <summary>
        /// Delete connection info from metadata table
        /// </summary>
        /// <param name="connInfo">Connection info as a struct</param>
        public async Task DeleteConnectionInfoAsync(ConnectionInfo connInfo)
            => await _connectionTableManager.DeleteConnection(connInfo.FromVertex, connInfo.FromEndpoint, connInfo.ToVertex, connInfo.ToEndpoint);

        public async Task<CRAErrorCode> ConnectAsync(
            string fromVertexName,
            string fromEndpoint,
            string toVertexName,
            string toEndpoint)
            => await ConnectAsync(
                fromVertexName,
                fromEndpoint,
                toVertexName,
                toEndpoint,
                ConnectionInitiator.FromSide);

        public async Task<CRAErrorCode> ConnectAsync(
            string fromVertexName,
            string fromEndpoint,
            string toVertexName,
            string toEndpoint,
            ConnectionInitiator direction)
        {
            // Tell from vertex to establish connection
            // Send request to CRA instance

            // Check that vertex and endpoints are valid and existing
            if (!await _vertexManager.ExistsVertex(fromVertexName)
                || !await _vertexManager.ExistsVertex(toVertexName))
            {
                // Check for sharded vertices
                List<int> fromVertexShards, toVertexShards;

                if ((fromVertexShards = await _vertexManager.ExistsShardedVertex(fromVertexName)).Count == 0)
                { return CRAErrorCode.VertexNotFound; }

                if ((toVertexShards = await _vertexManager.ExistsShardedVertex(toVertexName)).Count == 0)
                { return CRAErrorCode.VertexNotFound; }

                return ConnectSharded(fromVertexName, fromVertexShards, fromEndpoint, toVertexName, toVertexShards, toEndpoint, direction);
            }

            // Make the connection information stable
            await _connectionTableManager.AddConnection(fromVertexName, fromEndpoint, toVertexName, toEndpoint);

            // We now try best-effort to tell the CRA instance of this connection
            var result = CRAErrorCode.Success;

            VertexInfo? _row;
            var vertexInfoProvider = _vertexManager.VertexInfoProvider;
            try
            {
                // Get instance for source vertex
                _row = await (direction == ConnectionInitiator.FromSide
                    ? vertexInfoProvider.GetRowForActiveVertex(fromVertexName)
                    : vertexInfoProvider.GetRowForActiveVertex(toVertexName));
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
                    if (_localWorker.InstanceName == _row.Value.InstanceName)
                    {
                        return await _localWorker.Connect_InitiatorSide(
                            fromVertexName,
                            fromEndpoint,
                            toVertexName,
                            toEndpoint,
                            direction == ConnectionInitiator.ToSide);
                    }
                }


                // Send request to CRA instance
                TcpClient client = null;
                // Get address and port for instance, using row with vertex = ""
                var row = (await _vertexManager.GetRowForInstance(_row.Value.InstanceName)).Value;

                // Get a stream connection from the pool if available
                Stream stream;
                if (!TryGetSenderStreamFromPool(
                    row.Address,
                    row.Port.ToString(),
                    out stream))
                {
                    client = new TcpClient(row.Address, row.Port);
                    client.NoDelay = true;

                    stream = client.GetStream();

                    if (SecureStreamConnectionDescriptor != null)
                        stream = SecureStreamConnectionDescriptor.CreateSecureClient(stream, _row.Value.InstanceName);
                }

                if (direction == ConnectionInitiator.FromSide)
                { stream.WriteInt32((int)CRATaskMessageType.CONNECT_VERTEX_INITIATOR); }
                else
                { stream.WriteInt32((int)CRATaskMessageType.CONNECT_VERTEX_INITIATOR_REVERSE); }

                stream.WriteByteArray(Encoding.UTF8.GetBytes(fromVertexName));
                stream.WriteByteArray(Encoding.UTF8.GetBytes(fromEndpoint));
                stream.WriteByteArray(Encoding.UTF8.GetBytes(toVertexName));
                stream.WriteByteArray(Encoding.UTF8.GetBytes(toEndpoint));

                result = (CRAErrorCode)stream.ReadInt32();

                if (result != 0)
                { Console.WriteLine("Connection was logically established. However, the client received an error code from the connection-initiating CRA instance: " + result); }
                else
                { TryAddSenderStreamToPool(row.Address, row.Port.ToString(), stream); }
            }
            catch
            {
                Console.WriteLine("The connection-initiating CRA instance appears to be down or could not be found. Restart it and this connection will be completed automatically");
            }

            return result;
        }

        private CRAErrorCode ConnectSharded(
            string fromVertexName,
            List<int> fromVertexShards,
            string fromEndpoint,
            string toVertexName,
            List<int> toVertexShards,
            string toEndpoint,
            ConnectionInitiator direction)
        {
            var fromVertexNames = fromVertexShards
                .Select(e => fromVertexName + "$" + e)
                .ToArray();

            var toVertexNames = toVertexShards
                .Select(e => toVertexName + "$" + e)
                .ToArray();

            var fromEndpoints = toVertexShards
                .Select(e => fromEndpoint + "$" + e)
                .ToArray();

            var toEndpoints = fromVertexShards
                .Select(e => toEndpoint + "$" + e)
                .ToArray();

            return ConnectShardedVerticesWithFullMesh(
                    fromVertexNames,
                    fromEndpoints,
                    toVertexNames,
                    toEndpoints,
                    direction);
        }

        public async Task<string> GetDefaultInstanceNameAsync()
            => (await _vertexManager.GetRowForDefaultInstance()).InstanceName;

        /// <summary>
        /// Get a list of all output endpoint names for a given vertex
        /// </summary>
        /// <param name="vertexName"></param>
        /// <returns></returns>
        public async Task<IEnumerable<string>> GetOutputEndpointsAsync(string vertexName)
            => await _endpointTableManager.GetOutputEndpoints(vertexName);

        /// <summary>
        /// Get a list of all input endpoint names for a given vertex
        /// </summary>
        /// <param name="vertexName"></param>
        /// <returns></returns>
        public async Task<IEnumerable<string>> GetInputEndpointsAsync(string vertexName)
            => await _endpointTableManager.GetInputEndpoints(vertexName);

        /// <summary>
        /// Get all outgoing connection from a given vertex
        /// </summary>
        /// <param name="vertexName"></param>
        /// <returns></returns>
        public async Task<IEnumerable<ConnectionInfo>> GetConnectionsFromVertexAsync(string vertexName)
            => (await _connectionTableManager.GetConnectionsFromVertex(vertexName))
            .Select(_ =>
                new ConnectionInfo(
                    fromVertex: _.FromVertex,
                    fromEndpoint: _.FromEndpoint,
                    toVertex: _.ToVertex,
                    toEndpoint: _.ToEndpoint));

        /// <summary>
        /// Get all incoming connections to a given vertex
        /// </summary>
        /// <param name="vertexName"></param>
        /// <returns></returns>
        public async Task<IEnumerable<ConnectionInfo>> GetConnectionsToVertexAsync(string vertexName)
            => (await _connectionTableManager.GetConnectionsToVertex(vertexName))
            .Select(_ =>
                new ConnectionInfo(
                    fromVertex: _.FromVertex,
                    fromEndpoint: _.FromEndpoint,
                    toVertex: _.ToVertex,
                    toEndpoint: _.ToEndpoint));


        /// <summary>
        /// Gets a list of all vertices registered with CRA
        /// </summary>
        /// <returns></returns>
        public async Task<IEnumerable<string>> GetVertexNamesAsync()
            => await _vertexManager.GetVertexNames();

        /// <summary>
        /// Gets a list of all vertex definitions registered with CRA
        /// </summary>
        /// <returns></returns>
        public async Task<IEnumerable<string>> GetVertexDefinitionsAsync()
            => await _vertexManager.GetVertexDefinitions();

        /// <summary>
        /// Gets a list of all registered CRA instances
        /// </summary>
        /// <returns></returns>
        public async Task<IEnumerable<string>> GetInstanceNamesAsync()
            => await _vertexManager.GetInstanceNames();

        /// <summary>
        /// Disconnect a CRA connection
        /// </summary>
        /// <param name="fromVertexName"></param>
        /// <param name="fromVertexOutput"></param>
        /// <param name="toVertexName"></param>
        /// <param name="toVertexInput"></param>
        public async Task DisconnectAsync(string fromVertexName, string fromVertexOutput, string toVertexName, string toVertexInput)
            => await _connectionTableManager.DeleteConnection(fromVertexName, fromVertexOutput, toVertexName, toVertexInput);

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
