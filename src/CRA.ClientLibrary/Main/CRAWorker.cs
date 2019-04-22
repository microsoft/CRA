#define SHARDING

using CRA.ClientLibrary.AzureProvider;
using CRA.ClientLibrary.DataProvider;
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

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
        private readonly ConcurrentDictionary<string, IVertex> _localVertexTable = new ConcurrentDictionary<string, IVertex>();

        private readonly int _port;

        private readonly int _streamsPoolSize;
        private readonly string _workerinstanceName;
        private readonly IVertexInfoProvider _vertexInfoProvider;
        private readonly ConcurrentDictionary<string, CancellationTokenSource> inConnections = new ConcurrentDictionary<string, CancellationTokenSource>();
        private readonly ConcurrentDictionary<string, CancellationTokenSource> outConnections = new ConcurrentDictionary<string, CancellationTokenSource>();
        private readonly ConcurrentDictionary<string, ShardingInfo> shardingInfoTable = new ConcurrentDictionary<string, ShardingInfo>();

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

            _craClient = new CRAClientLibrary(azureDataProvider, this);

            _workerinstanceName = workerInstanceName;
            _address = address;
            _port = port;
            _streamsPoolSize = streamsPoolSize;

            _vertexInfoProvider = azureDataProvider.GetVertexInfoProvider();
            _connectionInfoProvider = azureDataProvider.GetVertexConnectionInfoProvider();

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

        /// <summary>
        /// Start the CRA worker. This method does not return.
        /// </summary>
        public void Start()
        {
            // Update vertex table
            _craClient.RegisterInstance(_workerinstanceName, _address, _port);

            // Then start server. This ensures that others can establish 
            // connections to local vertices at this point.
            Thread serverThread = new Thread(()=> StartServer().Wait());
            serverThread.IsBackground = true;
            serverThread.Start();

            // Wait for server to complete execution
            serverThread.Join();
        }

        internal async Task<CRAErrorCode> Connect_InitiatorSide(
            string fromVertexName,
            string fromVertexOutput,
            string toVertexName,
            string toVertexInput,
            bool reverse,
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
            var _row = (await _vertexInfoProvider.GetRowForInstanceVertex(row.InstanceName, ""))
                .Value;

            try
            {
                // Get a stream connection from the pool if available
                if (!_craClient.TryGetSenderStreamFromPool(_row.Address, _row.Port.ToString(), out ns))
                {
                    TcpClient client = new TcpClient(_row.Address, _row.Port);
                    client.NoDelay = true;

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
                                _row.Port));
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
                            _row.Port));

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
            int port = -1)
        {
            try
            {
                string key = fromVertexOutput;
                int shardId = -1;
#if SHARDING
                key = GetShardedVertexName(fromVertexOutput);
                shardId = GetShardedVertexShardId(fromVertexOutput);

                var skey = GetShardedVertexName(fromVertexName) + ":" + key + ":" + GetShardedVertexName(toVertexName);
                while (true)
                {
                    if (shardingInfoTable.ContainsKey(skey))
                    {
                        var si = shardingInfoTable[skey];
                        if (si.AllShards.Contains(GetShardedVertexShardId(toVertexName)))
                            break;
                        var newSI = await _craClient.GetShardingInfo(GetShardedVertexName(toVertexName));
                        if (shardingInfoTable.TryUpdate(skey, newSI, si))
                        {
                            ((IAsyncShardedVertexOutputEndpoint)_localVertexTable[fromVertexName].AsyncOutputEndpoints[key]).UpdateShardingInfo(GetShardedVertexName(toVertexName), newSI);
                            break;
                        }
                    }
                    else
                    {
                        var newSI = await _craClient.GetShardingInfo(GetShardedVertexName(toVertexName));
                        if (shardingInfoTable.TryAdd(skey, newSI))
                        {
                            ((IAsyncShardedVertexOutputEndpoint)_localVertexTable[fromVertexName].AsyncOutputEndpoints[key]).UpdateShardingInfo(GetShardedVertexName(toVertexName), newSI);
                            break;
                        }
                    }
                }
#endif
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

                    await _craClient.Disconnect(fromVertexName, fromVertexOutput, toVertexName, toVertexInput);
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
                        await fromVertex.ToInputAsync(toVertex, fromVertexName, fromVertexOutput, source.Token);
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
                    await _craClient.Disconnect(fromVertexName, fromVertexOutput, toVertexName, toVertexInput);
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
                    Task.Run(() => LoadVertex(stream));
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
            int port = -1)
        {
            try
            {
                string key = toVertexInput;
                int shardId = -1;
#if SHARDING
                key = GetShardedVertexName(toVertexInput);
                shardId = GetShardedVertexShardId(toVertexInput);

                var skey = GetShardedVertexName(toVertexName) + ":" + key + ":" + GetShardedVertexName(fromVertexName);
                while (true)
                {
                    if (shardingInfoTable.ContainsKey(skey))
                    {
                        var si = shardingInfoTable[skey];
                        if (si.AllShards.Contains(GetShardedVertexShardId(fromVertexName)))
                            break;
                        var newSI = await _craClient.GetShardingInfo(GetShardedVertexName(fromVertexName));
                        if (shardingInfoTable.TryUpdate(skey, newSI, si))
                        {
                            ((IAsyncShardedVertexInputEndpoint)_localVertexTable[toVertexName].AsyncInputEndpoints[key]).UpdateShardingInfo(GetShardedVertexName(fromVertexName), newSI);
                            break;
                        }
                    }
                    else
                    {
                        var newSI = await _craClient.GetShardingInfo(GetShardedVertexName(fromVertexName));
                        if (shardingInfoTable.TryAdd(skey, newSI))
                        {
                            ((IAsyncShardedVertexInputEndpoint)_localVertexTable[toVertexName].AsyncInputEndpoints[key]).UpdateShardingInfo(GetShardedVertexName(fromVertexName), newSI);
                            break;
                        }
                    }
                }
#endif
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

        private void LoadVertex(object streamObject)
        {
            var stream = (Stream)streamObject;

            string vertexName = Encoding.UTF8.GetString(stream.ReadByteArray());
            string vertexDefinition = Encoding.UTF8.GetString(stream.ReadByteArray());
            string vertexParam = Encoding.UTF8.GetString(stream.ReadByteArray());

            _craClient
                .LoadVertexAsync(vertexName, vertexDefinition, vertexParam, _workerinstanceName, _localVertexTable)
                .Wait();

            stream.WriteInt32(0);

            Task.Run(() => TryReuseReceiverStream(stream));
        }

        private async Task RestoreConnections(VertexInfo _row)
        {
            // Decide what to do if connection creation fails
            var outRows = await _connectionInfoProvider.GetAllConnectionsFromVertex(_row.VertexName);

            foreach (var row in outRows)
            {
                await RetryRestoreConnection(row.FromVertex, row.FromEndpoint, row.ToVertex, row.ToEndpoint, false);
            }

            var inRows = await _connectionInfoProvider.GetAllConnectionsFromVertex(_row.VertexName);
            foreach (var row in inRows)
            {
                await RetryRestoreConnection(row.FromVertex, row.FromEndpoint, row.ToVertex, row.ToEndpoint, true);
            }
        }

        private async void RestoreVertexAndConnections(VertexInfo _row)
        {
            await _craClient.LoadVertexAsync(_row.VertexName, _row.VertexDefinition, _row.VertexParameter, _workerinstanceName, _localVertexTable);
            await RestoreConnections(_row);
        }

        private async Task RestoreVerticesAndConnections()
        {
            var rows = await _vertexInfoProvider.GetAllRowsForInstance(_workerinstanceName);

            foreach (var _row in rows)
            {
                if (string.IsNullOrEmpty(_row.VertexName)) continue;
                RestoreVertexAndConnections(_row);
            }
        }

        private async Task RetryRestoreConnection(
            string fromVertexName,
            string fromVertexOutput,
            string toVertexName,
            string toVertexInput,
            bool reverse)
        {
            var conn = reverse ? inConnections : outConnections;

            bool killRemote = false;
            while (!conn.ContainsKey(fromVertexName + ":" + fromVertexOutput + ":" + toVertexName + ":" + toVertexInput))
            {
                if (!await _connectionInfoProvider.ContainsRow(
                    new VertexConnectionInfo(
                        fromVertex: fromVertexName,
                        fromEndpoint: fromVertexOutput,
                        toVertex: toVertexName,
                        toEndpoint: toVertexInput)))
                {
                    break;
                }


                Debug.WriteLine("Connecting " + fromVertexName + ":" + fromVertexOutput + ":" + toVertexName + ":" + toVertexInput);
                Debug.WriteLine("Connecting with killRemote set to " + (killRemote ? "true" : "false"));

                var result = await Connect_InitiatorSide(
                    fromVertexName,
                    fromVertexOutput,
                    toVertexName,
                    toVertexInput,
                    reverse,
                    false,
                    killRemote);

                if (result != 0)
                {
                    if (result == CRAErrorCode.ServerRecovering)
                    { killRemote = true; }

                    await Task.Delay(5000);
                }
                else
                    break;
            }
        }

        private async Task StartServer()
        {
            // Restore vertices on machine
            await RestoreVerticesAndConnections();

            var server = new TcpListener(IPAddress.Parse(_address), _port);

            // Start listening for client requests.
            server.Start();

            while (true)
            {
                Debug.WriteLine("Waiting for a connection... ");
                TcpClient client = server.AcceptTcpClient();
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

            if (_localVertexTable[fromVertexName].OutputEndpoints.ContainsKey(fromVertexOutput) &&
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
            else if (_localVertexTable[fromVertexName].AsyncOutputEndpoints.ContainsKey(fromVertexOutput) &&
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
