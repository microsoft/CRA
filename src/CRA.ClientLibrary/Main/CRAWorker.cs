#define SHARDING

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using CRA.DataProvider;

namespace CRA.ClientLibrary
{
    /// <summary>
    /// Worker library for Common Runtime for Applications (CRA)
    /// </summary>
    public class CRAWorker : IDisposable
    {
        private readonly string _address;

        private readonly IVertexConnectionInfoProvider _connectionInfoProvider;

        // CRA library client
        private readonly CRAClientLibrary _craClient;

        // Timer updateTimer
        internal readonly ConcurrentDictionary<string, IVertex> _localVertexTable = new ConcurrentDictionary<string, IVertex>();

        private readonly int _port;

        private readonly int _streamsPoolSize;
        private readonly string _workerinstanceName;
        private readonly IVertexInfoProvider _vertexInfoProvider;
        private readonly ConcurrentDictionary<string, CancellationTokenSource> inConnections = new ConcurrentDictionary<string, CancellationTokenSource>();
        private readonly ConcurrentDictionary<string, CancellationTokenSource> outConnections = new ConcurrentDictionary<string, CancellationTokenSource>();
        private readonly ConcurrentDictionary<string, ShardingInfo> shardingInfoTable = new ConcurrentDictionary<string, ShardingInfo>();

        /// <summary>
        /// Delay between connection retries
        /// </summary>
        private int _retryDelayMs = 5000;

        /// <summary>
        /// TCP connection timeout
        /// </summary>
        private int _tcpConnectTimeoutMs = 5000;

        /// <summary>
        /// Enable parallel connection establishment for a given vertex
        /// </summary>
        private bool _parallelConnect = false;

        /// <summary>
        /// Define a new worker instance of Common Runtime for Applications (CRA)
        /// </summary>
        /// <param name="workerInstanceName">Name of the worker instance</param>
        /// <param name="address">IP address</param>
        /// <param name="port">Port</param>
        /// <param name="storageConnectionString">Storage account to store metadata</param>
        /// <param name="streamsPoolSize">Maximum number of stream connections will be cached in the CRA client</param>
        /// <param name="descriptor">Secure stream connection callbacks</param>
        public CRAWorker(
            string workerInstanceName,
            string address,
            int port,
            IDataProvider azureDataProvider,
            ISecureStreamConnectionDescriptor descriptor = null,
            int streamsPoolSize = 0) :
            this(workerInstanceName, address, port, new CRAClientLibrary(azureDataProvider), descriptor, streamsPoolSize)
        {
        }

        /// <summary>
        /// Define a new worker instance of Common Runtime for Applications (CRA)
        /// </summary>
        /// <param name="workerInstanceName">Name of the worker instance</param>
        /// <param name="address">IP address</param>
        /// <param name="port">Port</param>
        /// <param name="clientLibrary">CRA client library instance</param>
        /// <param name="streamsPoolSize">Maximum number of stream connections will be cached in the CRA client</param>
        /// <param name="descriptor">Secure stream connection callbacks</param>
        public CRAWorker(
            string workerInstanceName,
            string address,
            int port,
            CRAClientLibrary clientLibrary,
            ISecureStreamConnectionDescriptor descriptor = null,
            int streamsPoolSize = 0)
        {
            Console.WriteLine("Starting CRA Worker instance [http://github.com/Microsoft/CRA]");
            Console.WriteLine("   Instance Name: " + workerInstanceName);
            Console.WriteLine("   IP address: " + address);
            Console.WriteLine("   Port: " + port);

            if (descriptor != null)
            { Console.WriteLine("   Secure network connections: Enabled using assembly " + descriptor.GetType().FullName); }
            else
            { Console.WriteLine("   Secure network connections: Disabled"); }

            _craClient = clientLibrary;
            _craClient.SetWorker(this);

            _workerinstanceName = workerInstanceName;
            _address = address;
            _port = port;
            _streamsPoolSize = streamsPoolSize;

            _vertexInfoProvider = _craClient.DataProvider.GetVertexInfoProvider();
            _connectionInfoProvider = _craClient.DataProvider.GetVertexConnectionInfoProvider();

            if (descriptor != null)
            { _craClient.SecureStreamConnectionDescriptor = descriptor; }
        }

        /// <summary>
        /// Instance name
        /// </summary>
        public string InstanceName { get { return _workerinstanceName; } }

        /// <summary>
        /// Streams pool size
        /// </summary>
        internal int StreamsPoolSize { get { return _streamsPoolSize; } }

        public void DisableDynamicLoading()
        {
            Console.WriteLine("Disabling dynamic assembly loading");
            _craClient.DisableDynamicLoading();
        }

        /// <summary>
        /// Set connection retry delay (in milliseconds)
        /// </summary>
        /// <param name="retryDelayMs"></param>
        public void SetConnectionRetryDelay(int retryDelayMs)
        {
            Console.WriteLine("Setting connection retry delay (ms) to {0}", retryDelayMs);
            _retryDelayMs = retryDelayMs;
        }

        /// <summary>
        /// Set TCP connection timeout (in milliseconds)
        /// </summary>
        /// <param name="tcpConnectTimeoutMs"></param>
        public void SetTcpConnectionTimeout(int tcpConnectTimeoutMs)
        {
            Console.WriteLine("Setting TCP connection timeout (ms) to {0}", tcpConnectTimeoutMs);
            _tcpConnectTimeoutMs = tcpConnectTimeoutMs;
        }


        /// <summary>
        /// Enable parallel connection establishment from given vertex
        /// </summary>
        public void EnableParallelConnections()
        {
            Console.WriteLine("Enabling parallel connection establishment");
            _parallelConnect = true;
        }

        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposeManaged)
        {
            if (!disposeManaged)
            { return; }

            _craClient.Dispose();
        }

        /// <summary>
        /// Kill the worker process (this process)
        /// </summary>
        /// <param name="killMessage">Kill message</param>
        public void Kill(string killMessage)
        {
            Console.WriteLine("KILLING WORKER: " + killMessage);
            Process.GetCurrentProcess().Kill();
        }

        public void SideloadVertex(IVertex vertex, string vertexName)
        {
            Console.WriteLine("Enabling sideload for vertex: " + vertexName + " (" + vertex.GetType().FullName + ")");
            _craClient.SideloadVertex(vertex, vertexName);
        }

        public async Task InstantSideloadVertexAsync(IVertex vertex, string vertexDefinition, string vertexName, bool loadParamFromMetadata = true, object param = null, bool loadConnectionsFromMetadata = true, IEnumerable<VertexConnectionInfo> outRows = null, IEnumerable<VertexConnectionInfo> inRows = null, bool waitForMetadata = false)
        {
            Console.WriteLine("Enabling sideload for vertex: " + vertexName + " (" + vertex.GetType().FullName + ")");
            
            if (loadParamFromMetadata)
                _craClient.SideloadVertex(vertex, vertexName);
            else
                _craClient.SideloadVertex(vertex, vertexName, param);

            if (!_localVertexTable.ContainsKey(vertexName))
            {
                await _craClient.LoadVertexAsync(vertexName, vertexDefinition, _workerinstanceName, _localVertexTable, false);

                if (loadConnectionsFromMetadata)
                {
                    if (outRows != null || inRows != null)
                    {
                        throw new Exception("Do not provide connection info if loadConnectionsFromMetadata is set");
                    }
                    outRows = await _connectionInfoProvider.GetAllConnectionsFromVertex(vertexName);
                    inRows = await _connectionInfoProvider.GetAllConnectionsToVertex(vertexName);
                }
                // Do not await this: happens in background
                var _ = RestoreConnections(outRows, inRows);
            }
            // Update metadata table with sideload information
            _craClient.DisableArtifactUploading();
            var taskList = new List<Task>();

            taskList.Add(_craClient.DefineVertexAsync(vertexDefinition, null));
            taskList.Add(_craClient.InstantiateVertexAsync(_workerinstanceName, vertexName, vertexDefinition, param, false, true, true, true));

            if (!loadConnectionsFromMetadata)
            {
                if (outRows != null)
                    foreach (var row in outRows)
                    {
                        taskList.Add(_craClient.ConnectAsync(row.FromVertex, row.FromEndpoint, row.ToVertex, row.ToEndpoint, ConnectionInitiator.FromSide, true, true, false));
                    }

                if (inRows != null)
                    foreach (var row in inRows)
                    {
                        taskList.Add(_craClient.ConnectAsync(row.FromVertex, row.FromEndpoint, row.ToVertex, row.ToEndpoint, ConnectionInitiator.FromSide, true, true, false));
                    }
            }

            if (waitForMetadata)
                await Task.WhenAll(taskList);
        }

        /// <summary>
        /// Start the CRA worker. This method does not return.
        /// </summary>
        public void Start()
        {
            // Update vertex table
            _craClient.RegisterInstance(_workerinstanceName, _address, _port);

            // Then start server. This ensures that others can establish 
            // connections to local vertices at this point.
            Thread serverThread = new Thread(() => StartServerAsync().Wait());
            serverThread.IsBackground = true;
            serverThread.Start();

            // Wait for server to complete execution
            serverThread.Join();
        }

        /// <summary>
        /// Start the CRA worker
        /// </summary>
        public async Task StartAsync()
        {
            Console.WriteLine($"Registering CRA instance {_workerinstanceName}");
            // Update vertex table
            _craClient.RegisterInstance(_workerinstanceName, _address, _port);
            Console.WriteLine("Finished registering CRA instance");
            // Then start server. This ensures that others can establish 
            // connections to local vertices at this point.
            await StartServerAsync();
        }

        internal async Task<CRAErrorCode> Connect_InitiatorSide(
            string fromVertexName,
            string fromVertexOutput,
            string toVertexName,
            string toVertexInput,
            bool reverse,
            bool sharding = true,
            bool killIfExists = true,
            bool killRemote = true)
        {
            VertexInfo row;

            try
            {
                // Need to get the latest address & port
                row = (await (reverse
                    ? _vertexInfoProvider.GetRowForActiveVertex(fromVertexName)
                    : _vertexInfoProvider.GetRowForActiveVertex(toVertexName))).Value;
            }
            catch
            {
                return CRAErrorCode.ActiveVertexNotFound;
            }

            // If from and to vertices are on the same (this) instance,
            // we can convert a "reverse" connection into a normal connection
            if (reverse && (row.InstanceName == InstanceName))
                reverse = false;

            CancellationTokenSource oldSource;
            var conn = reverse ? inConnections : outConnections;
            if (conn.TryGetValue(fromVertexName + ":" + fromVertexOutput + ":" + toVertexName + ":" + toVertexInput,
                out oldSource))
            {
                if (killIfExists)
                {
                    Debug.WriteLine("Deleting prior connection - it will automatically reconnect");
                    oldSource.Cancel();
                }
                return CRAErrorCode.Success;
            }

            if (TryFusedConnect(row.InstanceName, fromVertexName, fromVertexOutput, toVertexName, toVertexInput))
            {
                return CRAErrorCode.Success;
            }

            // Re-check the connection table as someone may have successfully
            // created a fused connection
            if (conn.TryGetValue(fromVertexName + ":" + fromVertexOutput + ":" + toVertexName + ":" + toVertexInput,
                out oldSource))
            {
                if (killIfExists)
                {
                    Debug.WriteLine("Deleting prior connection - it will automatically reconnect");
                    oldSource.Cancel();
                }
                return CRAErrorCode.Success;
            }

            // Send request to CRA instance
            Stream ns = null;
            VertexInfo _row = default;

            try
            {
                _row = (await _vertexInfoProvider.GetRowForInstanceVertex(row.InstanceName, "")).Value;

                // Get a stream connection from the pool if available
                if (!_craClient.TryGetSenderStreamFromPool(_row.Address, _row.Port.ToString(), out ns))
                {
                    var client = new TcpClient();
                    client.NoDelay = true;
                    await client.ConnectAsync(_row.Address, _row.Port, _tcpConnectTimeoutMs);

                    ns = _craClient.SecureStreamConnectionDescriptor
                          .CreateSecureClient(client.GetStream(), row.InstanceName);
                }
            }
            catch
            { return CRAErrorCode.ConnectionEstablishFailed; }

            if (!reverse)
                ns.WriteInt32((int)CRATaskMessageType.CONNECT_VERTEX_RECEIVER);
            else
                ns.WriteInt32((int)CRATaskMessageType.CONNECT_VERTEX_RECEIVER_REVERSE);

            ns.WriteByteArray(Encoding.UTF8.GetBytes(fromVertexName));
            ns.WriteByteArray(Encoding.UTF8.GetBytes(fromVertexOutput));
            ns.WriteByteArray(Encoding.UTF8.GetBytes(toVertexName));
            ns.WriteByteArray(Encoding.UTF8.GetBytes(toVertexInput));
            ns.WriteInt32(killRemote ? 1 : 0);
            CRAErrorCode result = (CRAErrorCode)ns.ReadInt32();

            if (result != 0)
            {
                Debug.WriteLine("Error occurs while establishing the connection!!");
                return result;
            }
            else
            {
                CancellationTokenSource source = new CancellationTokenSource();

                if (!reverse)
                {
                    if (outConnections.TryAdd(fromVertexName + ":" + fromVertexOutput + ":" + toVertexName + ":" + toVertexInput, source))
                    {
                        var tmp = Task.Run(() =>
                            EgressToStream(
                                fromVertexName,
                                fromVertexOutput,
                                toVertexName,
                                toVertexInput,
                                reverse,
                                ns,
                                source,
                                _row.Address,
                                _row.Port, 
                                sharding));
                        return CRAErrorCode.Success;
                    }
                    else
                    {
                        source.Dispose();
                        ns.Close();
                        Console.WriteLine("Race adding connection - deleting outgoing stream");
                        return CRAErrorCode.ConnectionAdditionRace;
                    }
                }
                else
                {
                    if (inConnections.TryAdd(fromVertexName + ":" + fromVertexOutput + ":" + toVertexName + ":" + toVertexInput, source))
                    {
                        var tmp = Task.Run(() => IngressFromStream(
                            fromVertexName,
                            fromVertexOutput,
                            toVertexName,
                            toVertexInput,
                            reverse,
                            ns,
                            source,
                            _row.Address,
                            _row.Port,
                            sharding));

                        return CRAErrorCode.Success;
                    }
                    else
                    {
                        source.Dispose();
                        ns.Close();
                        Debug.WriteLine("Race adding connection - deleting outgoing stream");
                        return CRAErrorCode.ConnectionAdditionRace;
                    }
                }
            }
        }

        private int Connect_ReceiverSide(
            string fromVertexName,
            string fromVertexOutput,
            string toVertexName,
            string toVertexInput,
            Stream stream,
            bool reverse,
            bool killIfExists = true)
        {
            CancellationTokenSource oldSource;
            var conn = reverse ? outConnections : inConnections;
            if (conn.TryGetValue(fromVertexName + ":" + fromVertexOutput + ":" + toVertexName + ":" + toVertexInput, out oldSource))
            {
                if (killIfExists)
                {
                    Debug.WriteLine("Deleting prior connection - it will automatically reconnect");
                    oldSource.Cancel();
                }
                else
                {
                    Debug.WriteLine("There exists prior connection - not killing");
                }
                stream.WriteInt32((int)CRAErrorCode.ServerRecovering);
                return (int)CRAErrorCode.ServerRecovering;
            }
            else
            {
                stream.WriteInt32(0);
            }

            CancellationTokenSource source = new CancellationTokenSource();

            if (!reverse)
            {
                if (inConnections.TryAdd(fromVertexName + ":" + fromVertexOutput + ":" + toVertexName + ":" + toVertexInput, source))
                {
                    Task.Run(() =>
                        IngressFromStream(
                            fromVertexName,
                            fromVertexOutput,
                            toVertexName,
                            toVertexInput,
                            reverse,
                            stream,
                            source));

                    return (int)CRAErrorCode.Success;
                }
                else
                {
                    source.Dispose();
                    stream.Close();
                    Debug.WriteLine("Race adding connection - deleting incoming stream");
                    return (int)CRAErrorCode.ConnectionAdditionRace;
                }
            }
            else
            {
                if (outConnections.TryAdd(fromVertexName + ":" + fromVertexOutput + ":" + toVertexName + ":" + toVertexInput, source))
                {
                    Task.Run(() =>
                        EgressToStream(
                            fromVertexName,
                            fromVertexOutput,
                            toVertexName,
                            toVertexInput,
                            reverse,
                            stream,
                            source));

                    return (int)CRAErrorCode.Success;
                }
                else
                {
                    source.Dispose();
                    stream.Close();
                    Debug.WriteLine("Race adding connection - deleting incoming stream");
                    return (int)CRAErrorCode.ConnectionAdditionRace;
                }
            }

        }

        private async Task ConnectVertex_Initiator(object streamObject, bool reverse = false)
        {
            var stream = (Stream)streamObject;

            string fromVertexName = Encoding.UTF8.GetString(stream.ReadByteArray());
            string fromVertexOutput = Encoding.UTF8.GetString(stream.ReadByteArray());
            string toVertexName = Encoding.UTF8.GetString(stream.ReadByteArray());
            string toVertexInput = Encoding.UTF8.GetString(stream.ReadByteArray());

            Debug.WriteLine("Processing request to initiate connection");

            if (!reverse)
            {
                if (!_localVertexTable.ContainsKey(fromVertexName))
                {
                    stream.WriteInt32((int)CRAErrorCode.VertexNotFound);
                    await TryReuseReceiverStream(stream);
                    return;
                }

                var key = GetShardedVertexName(fromVertexOutput);
                if (!_localVertexTable[fromVertexName].OutputEndpoints.ContainsKey(key) &&
                    !_localVertexTable[fromVertexName].AsyncOutputEndpoints.ContainsKey(key)
                   )
                {
                    stream.WriteInt32((int)CRAErrorCode.VertexInputNotFound);
                    await TryReuseReceiverStream(stream);
                    return;
                }
            }
            else
            {
                if (!_localVertexTable.ContainsKey(toVertexName))
                {
                    stream.WriteInt32((int)CRAErrorCode.VertexNotFound);
                    await TryReuseReceiverStream(stream);
                    return;
                }

                if (!_localVertexTable[toVertexName].InputEndpoints.ContainsKey(toVertexInput) &&
                    !_localVertexTable[toVertexName].AsyncInputEndpoints.ContainsKey(toVertexInput)
                    )
                {
                    stream.WriteInt32((int)CRAErrorCode.VertexInputNotFound);
                    await TryReuseReceiverStream(stream);
                    return;
                }
            }

            CRAErrorCode result = await Connect_InitiatorSide(
                fromVertexName,
                fromVertexOutput,
                toVertexName,
                toVertexInput,
                reverse);

            stream.WriteInt32((int)result);

            await TryReuseReceiverStream(stream);
        }

        private void ConnectVertex_Receiver(object streamObject, bool reverse = false)
        {
            var stream = (Stream)streamObject;

            string fromVertexName = Encoding.UTF8.GetString(stream.ReadByteArray());
            string fromVertexOutput = Encoding.UTF8.GetString(stream.ReadByteArray());
            string toVertexName = Encoding.UTF8.GetString(stream.ReadByteArray());
            string toVertexInput = Encoding.UTF8.GetString(stream.ReadByteArray());
            bool killIfExists = stream.ReadInt32() == 1 ? true : false;

            if (!reverse)
            {
                if (!_localVertexTable.ContainsKey(toVertexName))
                {
                    stream.WriteInt32((int)CRAErrorCode.VertexNotFound);
                    Task.Run(() => TryReuseReceiverStream(stream));
                    return;
                }

                var key = GetShardedVertexName(toVertexInput);
                if (!_localVertexTable[toVertexName].InputEndpoints.ContainsKey(key) &&
                    !_localVertexTable[toVertexName].AsyncInputEndpoints.ContainsKey(key)
                    )
                {
                    stream.WriteInt32((int)CRAErrorCode.VertexInputNotFound);
                    Task.Run(() => TryReuseReceiverStream(stream));
                    return;
                }
            }
            else
            {
                if (!_localVertexTable.ContainsKey(fromVertexName))
                {
                    stream.WriteInt32((int)CRAErrorCode.VertexNotFound);
                    Task.Run(() => TryReuseReceiverStream(stream));
                    return;
                }

                if (!_localVertexTable[fromVertexName].OutputEndpoints.ContainsKey(fromVertexOutput) &&
                    !_localVertexTable[fromVertexName].AsyncOutputEndpoints.ContainsKey(fromVertexOutput)
                    )
                {
                    stream.WriteInt32((int)CRAErrorCode.VertexInputNotFound);
                    Task.Run(() => TryReuseReceiverStream(stream));
                    return;
                }
            }

            int result = Connect_ReceiverSide(
                fromVertexName,
                fromVertexOutput,
                toVertexName,
                toVertexInput,
                stream,
                reverse,
                killIfExists);

            // Do not close and dispose stream because it is being reused for data
            if (result != 0)
            {
                Task.Run(() => TryReuseReceiverStream(stream));
            }
        }

        private async Task EgressToStream(
            string fromVertexName,
            string fromVertexOutput,
            string toVertexName,
            string toVertexInput,
            bool reverse,
            Stream ns,
            CancellationTokenSource source,
            string address = null,
            int port = -1,
            bool sharding = true)
        {
            try
            {
                string key = fromVertexOutput;
                int shardId = -1;
//#if SHARDING
                if (sharding)
                {
                    key = GetShardedVertexName(fromVertexOutput);
                    shardId = GetShardedVertexShardId(fromVertexOutput);

                    if (shardId >= 0)
                    {
                        var skey = GetShardedVertexName(fromVertexName) + ":" + key + ":" + GetShardedVertexName(toVertexName);
                        while (true)
                        {
                            if (shardingInfoTable.ContainsKey(skey))
                            {
                                var si = shardingInfoTable[skey];
                                if (si.AllShards.Contains(GetShardedVertexShardId(toVertexName)))
                                    break;
                                var newSI = await _craClient.GetShardingInfoAsync(GetShardedVertexName(toVertexName));
                                if (shardingInfoTable.TryUpdate(skey, newSI, si))
                                {
                                    ((IAsyncShardedVertexOutputEndpoint)_localVertexTable[fromVertexName].AsyncOutputEndpoints[key]).UpdateShardingInfo(GetShardedVertexName(toVertexName), newSI);
                                    break;
                                }
                            }
                            else
                            {
                                var newSI = await _craClient.GetShardingInfoAsync(GetShardedVertexName(toVertexName));
                                if (shardingInfoTable.TryAdd(skey, newSI))
                                {
                                    ((IAsyncShardedVertexOutputEndpoint)_localVertexTable[fromVertexName].AsyncOutputEndpoints[key]).UpdateShardingInfo(GetShardedVertexName(toVertexName), newSI);
                                    break;
                                }
                            }
                        }
                    }
                }
//#endif
                if (_localVertexTable[fromVertexName].OutputEndpoints.ContainsKey(key))
                {
                    if (shardId < 0)
                        await
                            Task.Run(() =>
                                _localVertexTable[fromVertexName].OutputEndpoints[fromVertexOutput]
                                    .ToStream(ns, toVertexName, toVertexInput, source.Token), source.Token);
                    else
                        throw new NotImplementedException();

                }
                else if (_localVertexTable[fromVertexName].AsyncOutputEndpoints.ContainsKey(key))
                {
                    if (shardId < 0)
                        await _localVertexTable[fromVertexName].AsyncOutputEndpoints[fromVertexOutput].ToStreamAsync(ns, toVertexName, toVertexInput, source.Token);
                    else
                        await ((IAsyncShardedVertexOutputEndpoint)_localVertexTable[fromVertexName].AsyncOutputEndpoints[key])
                            .ToStreamAsync(ns, GetShardedVertexName(toVertexName),
                            shardId, GetShardedVertexName(toVertexInput), source.Token);
                }
                else
                {
                    Debug.WriteLine("Unable to find output endpoint (on from side)");
                    return;
                }

                CancellationTokenSource oldSource;
                if (outConnections.TryRemove(fromVertexName + ":" + fromVertexOutput + ":" + toVertexName + ":" + toVertexInput, out oldSource))
                {
                    oldSource.Dispose();

                    if (address != null && port != -1)
                    {
                        // Add/Return a sender stream connection to the pool
                        if (!_craClient.TryAddSenderStreamToPool(address, port.ToString(), (NetworkStream)ns))
                        {
                            ns.Dispose();
                        }
                    }
                    else
                    {
                        // Keep a receiver stream connection to be used later
#pragma warning disable CS4014
                        Task.Run(() => TryReuseReceiverStream(ns));
#pragma warning restore CS4014
                    }

                    await _craClient.DisconnectAsync(fromVertexName, fromVertexOutput, toVertexName, toVertexInput);
                }
            }
            catch (Exception e)
            {
                Debug.WriteLine("Exception (" + e.ToString() + ") in outgoing stream - reconnecting");
                CancellationTokenSource oldSource;
                if (outConnections.TryRemove(fromVertexName + ":" + fromVertexOutput + ":" + toVertexName + ":" + toVertexInput, out oldSource))
                {
                    oldSource.Dispose();
                }
                else
                {
                    Debug.WriteLine("Unexpected: caught exception in ToStream but entry absent in outConnections");
                }

                // Retry following while connection not in list
                await RetryRestoreConnection(fromVertexName, fromVertexOutput, toVertexName, toVertexInput, false);
            }
        }

        private async Task EgressToVertexInput(string fromVertexName, string fromVertexOutput, string toVertexName, string toVertexInput,
            CancellationTokenSource source)
        {
            try
            {
                if (_localVertexTable[fromVertexName].OutputEndpoints.ContainsKey(fromVertexOutput))
                {
                    var fromVertex = _localVertexTable[fromVertexName].OutputEndpoints[fromVertexOutput] as IFusableVertexOutputEndpoint;
                    var toVertex = _localVertexTable[toVertexName].InputEndpoints[toVertexInput] as IVertexInputEndpoint;

                    if (fromVertex != null && toVertex != null && fromVertex.CanFuseWith(toVertex, toVertexName, toVertexInput))
                    {
                        await
                            Task.Run(() => fromVertex.ToInput(toVertex, toVertexName, toVertexInput, source.Token));
                    }
                    else
                    {
                        Debug.WriteLine("Unable to create fused connection");
                        return;
                    }
                }
                else if (_localVertexTable[fromVertexName].AsyncOutputEndpoints.ContainsKey(fromVertexOutput))
                {
                    var fromVertex = _localVertexTable[fromVertexName].AsyncOutputEndpoints[fromVertexOutput] as IAsyncFusableVertexOutputEndpoint;
                    var toVertex = _localVertexTable[toVertexName].AsyncInputEndpoints[toVertexInput] as IAsyncVertexInputEndpoint;

                    if (fromVertex != null && toVertex != null && fromVertex.CanFuseWith(toVertex, toVertexName, toVertexInput))
                    {
                        await fromVertex.ToInputAsync(toVertex, toVertexName, toVertexInput, source.Token);
                    }
                    else
                    {
                        Debug.WriteLine("Unable to create fused connection");
                        return;
                    }
                }
                else
                {
                    Debug.WriteLine("Unable to create fused connection");
                    return;
                }

                CancellationTokenSource oldSource;
                if (outConnections.TryRemove(fromVertexName + ":" + fromVertexOutput + ":" + toVertexName + ":" + toVertexInput, out oldSource))
                {
                    oldSource.Dispose();
                    await _craClient.DisconnectAsync(fromVertexName, fromVertexOutput, toVertexName, toVertexInput);
                }
            }
            catch (Exception e)
            {
                Debug.WriteLine("Exception (" + e.ToString() + ") in outgoing stream - reconnecting");
                CancellationTokenSource oldSource;
                if (outConnections.TryRemove(fromVertexName + ":" + fromVertexOutput + ":" + toVertexName + ":" + toVertexInput, out oldSource))
                {
                    oldSource.Dispose();
                }
                else
                {
                    Debug.WriteLine("Unexpected: caught exception in ToStream but entry absent in outConnections");
                }

                // Retry following while connection not in list
                await RetryRestoreConnection(fromVertexName, fromVertexOutput, toVertexName, toVertexInput, false);
            }
        }

        private string GetShardedVertexName(string name)
        {
            if (name.Contains("$"))
                return name.Split('$')[0];
            return name;
        }

        private int GetShardedVertexShardId(string name)
        {
            if (name.Contains("$"))
                return int.Parse(name.Split('$')[1]);
            return -1;
        }

        private void HandleCRATaskMessage(CRATaskMessageType message, Stream stream)
        {
            switch (message)
            {
                case CRATaskMessageType.LOAD_VERTEX:
                    Task.Run(() => LoadVertexAsync(stream));
                    break;

                case CRATaskMessageType.CONNECT_VERTEX_INITIATOR:
                    Task.Run(() => ConnectVertex_Initiator(stream, false));
                    break;

                case CRATaskMessageType.CONNECT_VERTEX_RECEIVER:
                    Task.Run(() => ConnectVertex_Receiver(stream, false));
                    break;

                case CRATaskMessageType.CONNECT_VERTEX_INITIATOR_REVERSE:
                    Task.Run(() => ConnectVertex_Initiator(stream, true));
                    break;

                case CRATaskMessageType.CONNECT_VERTEX_RECEIVER_REVERSE:
                    Task.Run(() => ConnectVertex_Receiver(stream, true));
                    break;

                default:
                    Console.WriteLine("Unknown message type: " + message);
                    break;
            }
        }

        private async Task IngressFromStream(
            string fromVertexName,
            string fromVertexOutput,
            string toVertexName,
            string toVertexInput,
            bool reverse,
            Stream ns,
            CancellationTokenSource source,
            string address = null,
            int port = -1,
            bool sharding = true)
        {
            try
            {
                string key = toVertexInput;
                int shardId = -1;
//#if SHARDING
                if (sharding)
                {
                    key = GetShardedVertexName(toVertexInput);
                    shardId = GetShardedVertexShardId(toVertexInput);

                    if (shardId >= 0)
                    {
                        var skey = GetShardedVertexName(toVertexName) + ":" + key + ":" + GetShardedVertexName(fromVertexName);
                        while (true)
                        {
                            if (shardingInfoTable.ContainsKey(skey))
                            {
                                var si = shardingInfoTable[skey];
                                if (si.AllShards.Contains(GetShardedVertexShardId(fromVertexName)))
                                    break;
                                var newSI = await _craClient.GetShardingInfoAsync(GetShardedVertexName(fromVertexName));
                                if (shardingInfoTable.TryUpdate(skey, newSI, si))
                                {
                                    ((IAsyncShardedVertexInputEndpoint)_localVertexTable[toVertexName].AsyncInputEndpoints[key]).UpdateShardingInfo(GetShardedVertexName(fromVertexName), newSI);
                                    break;
                                }
                            }
                            else
                            {
                                var newSI = await _craClient.GetShardingInfoAsync(GetShardedVertexName(fromVertexName));
                                if (shardingInfoTable.TryAdd(skey, newSI))
                                {
                                    ((IAsyncShardedVertexInputEndpoint)_localVertexTable[toVertexName].AsyncInputEndpoints[key]).UpdateShardingInfo(GetShardedVertexName(fromVertexName), newSI);
                                    break;
                                }
                            }
                        }
                    }
                }
//#endif
                if (_localVertexTable[toVertexName].InputEndpoints.ContainsKey(key))
                {
                    if (shardId < 0)
                        await Task.Run(
                            () => _localVertexTable[toVertexName].InputEndpoints[toVertexInput]
                            .FromStream(ns, fromVertexName, fromVertexOutput, source.Token), source.Token);
                    else
                        throw new NotImplementedException();
                }
                else if (_localVertexTable[toVertexName].AsyncInputEndpoints.ContainsKey(key))
                {
                    if (shardId < 0)
                        await _localVertexTable[toVertexName].AsyncInputEndpoints[toVertexInput].FromStreamAsync(ns, fromVertexName, fromVertexOutput, source.Token);
                    else
                        await ((IAsyncShardedVertexInputEndpoint)_localVertexTable[toVertexName].AsyncInputEndpoints[key])
                            .FromStreamAsync(ns, GetShardedVertexName(fromVertexName),
                            shardId, GetShardedVertexName(fromVertexOutput), source.Token);
                }
                else
                {
                    Debug.WriteLine("Unable to find input endpoint (on to side)");
                    return;
                }

                // Completed FromStream successfully
                CancellationTokenSource oldSource;
                if (inConnections.TryRemove(fromVertexName + ":" + fromVertexOutput + ":" + toVertexName + ":" + toVertexInput, out oldSource))
                {
                    oldSource.Dispose();

                    if (address != null && port != -1)
                    {
                        // Add/Return a sender stream connection to the pool
                        if (!_craClient.TryAddSenderStreamToPool(address, port.ToString(), (NetworkStream)ns))
                        {
                            ns.Dispose();
                        }
                    }
                    else
                    {
                        // Keep a receiver stream connection to be used later
#pragma warning disable CS4014
                        Task.Run(() => TryReuseReceiverStream(ns));
#pragma warning restore CS4014
                    }
                }
            }
            catch (Exception e)
            {
                Debug.WriteLine("Exception (" + e.ToString() + ") in incoming stream - reconnecting");
                CancellationTokenSource tokenSource;
                if (inConnections.TryRemove(fromVertexName + ":" + fromVertexOutput + ":" + toVertexName + ":" + toVertexInput, out tokenSource))
                {
                    tokenSource.Dispose();
                }
                else
                {
                    Debug.WriteLine("Unexpected: caught exception in FromStream but entry absent in inConnections");
                }

                await RetryRestoreConnection(fromVertexName, fromVertexOutput, toVertexName, toVertexInput, true);
            }
        }

        private async Task LoadVertexAsync(object streamObject)
        {
            var stream = (Stream)streamObject;

            string vertexName = Encoding.UTF8.GetString(stream.ReadByteArray());
            string vertexDefinition = Encoding.UTF8.GetString(stream.ReadByteArray());

            await _craClient
                .LoadVertexAsync(vertexName, vertexDefinition, _workerinstanceName, _localVertexTable, true);

            stream.WriteInt32(0);

            Task.Run(() => TryReuseReceiverStream(stream));
        }

        private async Task RestoreConnections(IEnumerable<VertexConnectionInfo> outRows, IEnumerable<VertexConnectionInfo> inRows)
        {            
            var outQueue = new Queue<VertexConnectionInfo>();

            if (outRows != null)
                foreach (var row in outRows)
                    outQueue.Enqueue(row);

            var inQueue = new Queue<VertexConnectionInfo>();

            if (inRows != null)
                foreach (var row in inRows)
                    inQueue.Enqueue(row);

            while (outQueue.Count > 0 || inQueue.Count > 0)
            {
                if (outQueue.Count > 0)
                {
                    var row = outQueue.Dequeue();
                    var task = RetryRestoreConnection(row.FromVertex, row.FromEndpoint, row.ToVertex, row.ToEndpoint, false, _parallelConnect);
                    if (!_parallelConnect)
                    {
                        bool done = await task;
                        if (!done)
                        {
                            outQueue.Enqueue(row);
                        }
                    }
                }

                if (inQueue.Count > 0)
                {
                    var row = inQueue.Dequeue();
                    var task = RetryRestoreConnection(row.FromVertex, row.FromEndpoint, row.ToVertex, row.ToEndpoint, true, _parallelConnect);
                    if (!_parallelConnect)
                    {
                        bool done = await task;
                        if (!done)
                        {
                            inQueue.Enqueue(row);
                        }
                    }
                }
            }
        }

        private async Task RestoreConnections(VertexInfo _row)
        {
            var outRows = await _connectionInfoProvider.GetAllConnectionsFromVertex(_row.VertexName);
            var inRows = await _connectionInfoProvider.GetAllConnectionsToVertex(_row.VertexName);
            await RestoreConnections(outRows, inRows);
        }

        internal async Task RestoreVertexAndConnectionsAsync(VertexInfo _row)
        {
            if (!_localVertexTable.ContainsKey(_row.VertexName))
            {
                await _craClient.LoadVertexAsync(_row.VertexName, _row.VertexDefinition, _workerinstanceName, _localVertexTable, true);
                await RestoreConnections(_row);
            }
        }

        private async Task RestoreVerticesAndConnectionsAsync()
        {
            var rows = await _vertexInfoProvider.GetAllRowsForInstance(_workerinstanceName);

            foreach (var _row in rows)
            {
                if (string.IsNullOrEmpty(_row.VertexName)) continue;
                // Restore vertices in parallel tasks
                var _ = RestoreVertexAndConnectionsAsync(_row);
            }
        }

        private async Task<bool> RetryRestoreConnection(
            string fromVertexName,
            string fromVertexOutput,
            string toVertexName,
            string toVertexInput,
            bool reverse,
            bool retryForever = true)
        {
            var conn = reverse ? inConnections : outConnections;

            bool killRemote = false;
            bool first = true;

            while (!conn.ContainsKey(fromVertexName + ":" + fromVertexOutput + ":" + toVertexName + ":" + toVertexInput))
            {
                if (!first && !await _connectionInfoProvider.ContainsRow(
                    new VertexConnectionInfo(
                        fromVertex: fromVertexName,
                        fromEndpoint: fromVertexOutput,
                        toVertex: toVertexName,
                        toEndpoint: toVertexInput)))
                {
                    break;
                }
                first = false;

                Debug.WriteLine("Connecting " + fromVertexName + ":" + fromVertexOutput + ":" + toVertexName + ":" + toVertexInput);
                Debug.WriteLine("Connecting with killRemote set to " + (killRemote ? "true" : "false"));

                var result = await Connect_InitiatorSide(
                    fromVertexName,
                    fromVertexOutput,
                    toVertexName,
                    toVertexInput,
                    reverse,
                    true,
                    false,
                    killRemote);

                if (result != 0)
                {
                    if (result == CRAErrorCode.ServerRecovering)
                    {
                        if (killRemote && !retryForever)
                            return false;
                        killRemote = true;
                        await Task.Delay(_retryDelayMs);
                    }
                    else
                    {
                        await Task.Delay(_retryDelayMs);
                        if (!retryForever)
                            return false;
                    }
                }
                else
                    break;
            }
            return true;
        }

        private async Task StartServerAsync()
        {
            // Restore vertices on machine
            await RestoreVerticesAndConnectionsAsync();

            var server = new TcpListener(IPAddress.Parse(_address), _port);

            // Start listening for client requests.
            server.Start();
           
            while (true)
            {
                Debug.WriteLine("Waiting for a connection... ");
                TcpClient client = await server.AcceptTcpClientAsync();
                client.NoDelay = true;

                Debug.WriteLine("Connected!");

                // Get a stream object for reading and writing
                Stream stream = _craClient.SecureStreamConnectionDescriptor.CreateSecureServer(client.GetStream());

                // Handle a task message sent to the CRA instance
                CRATaskMessageType message = (CRATaskMessageType)stream.ReadInt32();
                HandleCRATaskMessage(message, stream);
            }
        }

        private bool TryFusedConnect(
            string otherInstance,
            string fromVertexName,
            string fromVertexOutput,
            string toVertexName,
            string toVertexInput)
        {
            if (otherInstance != InstanceName)
                return false;

            CancellationTokenSource source = new CancellationTokenSource();

            if (_localVertexTable.ContainsKey(fromVertexName) &&
                _localVertexTable[fromVertexName].OutputEndpoints.ContainsKey(fromVertexOutput) &&
                _localVertexTable.ContainsKey(toVertexName) &&
                _localVertexTable[toVertexName].InputEndpoints.ContainsKey(toVertexInput))
            {
                var fromVertex = _localVertexTable[fromVertexName].OutputEndpoints[fromVertexOutput] as IFusableVertexOutputEndpoint;
                var toVertex = _localVertexTable[toVertexName].InputEndpoints[toVertexInput] as IVertexInputEndpoint;

                if (fromVertex != null && toVertex != null && fromVertex.CanFuseWith(toVertex, toVertexName, toVertexInput))
                {
                    if (outConnections.TryAdd(fromVertexName + ":" + fromVertexOutput + ":" + toVertexName + ":" + toVertexInput, source))
                    {
                        Task.Run(() => EgressToVertexInput(fromVertexName, fromVertexOutput, toVertexName, toVertexInput, source));
                        return true;
                    }
                    else
                        return false;
                }
                else
                    return false;
            }
            else if (_localVertexTable.ContainsKey(fromVertexName) &&
                _localVertexTable[fromVertexName].AsyncOutputEndpoints.ContainsKey(fromVertexOutput) &&
                _localVertexTable.ContainsKey(toVertexName) &&
                _localVertexTable[toVertexName].AsyncInputEndpoints.ContainsKey(toVertexInput))
            {
                var fromVertex = _localVertexTable[fromVertexName].AsyncOutputEndpoints[fromVertexOutput] as IAsyncFusableVertexOutputEndpoint;
                var toVertex = _localVertexTable[toVertexName].AsyncInputEndpoints[toVertexInput] as IAsyncVertexInputEndpoint;

                if (fromVertex != null && toVertex != null && fromVertex.CanFuseWith(toVertex, toVertexName, toVertexInput))
                {
                    if (outConnections.TryAdd(fromVertexName + ":" + fromVertexOutput + ":" + toVertexName + ":" + toVertexInput, source))
                    {
                        Task.Run(() => EgressToVertexInput(fromVertexName, fromVertexOutput, toVertexName, toVertexInput, source));
                        return true;
                    }
                    else
                        return false;
                }
                else
                {
                    return false;
                }
            }
            else
            {
                return false;
            }
        }

        private async Task TryReuseReceiverStream(Stream stream)
        {
            try
            {
                CRATaskMessageType message = (CRATaskMessageType)(await stream.ReadInt32Async());
                if (message == CRATaskMessageType.PING)
                {
                    stream.WriteInt32(0);

                    // Handle a task message sent to the CRA instance
                    CRATaskMessageType newMessage = (CRATaskMessageType)stream.ReadInt32();
                    HandleCRATaskMessage(newMessage, (NetworkStream)stream);
                }
                else
                    stream.Close();
            }
            catch (Exception)
            {
                stream.Close();
            }
        }
    }
}
