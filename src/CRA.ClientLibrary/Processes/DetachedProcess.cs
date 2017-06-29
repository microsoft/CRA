using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Text;

namespace CRA.ClientLibrary
{
    /// <summary>
    /// All connections to/from this detached process
    /// </summary>
    public class ConnectionData
    {
        /// <summary>
        /// Input endpoints
        /// </summary>
        public ConcurrentDictionary<ConnectionInfo, Stream> InputConnections { get; }

        /// <summary>
        /// Output endpoints
        /// </summary>
        public ConcurrentDictionary<ConnectionInfo, Stream> OutputConnections { get; }

        /// <summary>
        /// 
        /// </summary>
        public ConnectionData()
        {
            InputConnections = new ConcurrentDictionary<ConnectionInfo, Stream>();
            OutputConnections = new ConcurrentDictionary<ConnectionInfo, Stream>();
        }
    }

    /// <summary>
    /// Endpoint information for process
    /// </summary>
    public class EndpointData
    {
        /// <summary>
        /// Input endpoints
        /// </summary>
        public ConcurrentDictionary<string, bool> InputEndpoints { get; }

        /// <summary>
        /// Output endpoints
        /// </summary>
        public ConcurrentDictionary<string, bool> OutputEndpoints { get; }

        /// <summary>
        /// Constructor
        /// </summary>
        public EndpointData()
        {
            InputEndpoints = new ConcurrentDictionary<string, bool>();
            OutputEndpoints = new ConcurrentDictionary<string, bool>();
        }
    }

    /// <summary>
    /// Process proxy for applications using CRA sideways
    /// </summary>
    public class DetachedProcess : IDisposable
    {
        /// <summary>
        /// Connection data
        /// </summary>
        public ConnectionData ConnectionData { get; set; }

        /// <summary>
        /// Endpoint data
        /// </summary>
        public EndpointData EndpointData { get; set; }

        private CRAClientLibrary _clientLibrary;
        private string _processName;
        private string _instanceName;
        private bool _isEphemeralInstance;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="processName"></param>
        /// <param name="instanceName"></param>
        /// <param name="clientLibrary"></param>
        public DetachedProcess(string processName, string instanceName, CRAClientLibrary clientLibrary)
        {
            _processName = processName;
            _clientLibrary = clientLibrary;

            if (instanceName == "")
            {
                // _clientLibrary._processTableManager.GetRowForProcess(processName);
                _instanceName = RandomString(16);
                _isEphemeralInstance = true;
                _clientLibrary.RegisterInstance(_instanceName, "", 0);
            }

            _clientLibrary._processTableManager.RegisterProcess(_processName, _instanceName);

            EndpointData = new EndpointData();
            ConnectionData = new ConnectionData();
        }

        /// <summary>
        /// Add input endpoint
        /// </summary>
        /// <param name="endpointName">Endpoint name</param>
        public void AddInputEndpoint(string endpointName)
        {
            _clientLibrary.AddEndpoint(_processName, endpointName, true, false);
            EndpointData.InputEndpoints.TryAdd(endpointName, true);
        }

        /// <summary>
        /// Add output endpoint
        /// </summary>
        /// <param name="endpointName">Endpoint name</param>
        public void AddOutputEndpoint(string endpointName)
        {
            _clientLibrary.AddEndpoint(_processName, endpointName, false, false);
            EndpointData.OutputEndpoints.TryAdd(endpointName, true);
        }

        /// <summary>
        /// Create connection stream from remote output endpoint to local input endpoint
        /// </summary>
        /// <param name="localInputEndpointName"></param>
        /// <param name="remoteProcess"></param>
        /// <param name="remoteOutputEndpoint"></param>
        /// <returns></returns>
        public Stream FromRemoteOutputEndpointStream(string localInputEndpointName, string remoteProcess, string remoteOutputEndpoint)
        {
            AddInputEndpoint(localInputEndpointName);

            _clientLibrary.AddConnectionInfo(remoteProcess, remoteOutputEndpoint, _processName, localInputEndpointName);
            var stream = Connect_InitiatorSide(remoteProcess, remoteOutputEndpoint, _processName, localInputEndpointName, true);
            var conn = new ConnectionInfo(remoteProcess, remoteOutputEndpoint, _processName, localInputEndpointName);
            ConnectionData.InputConnections.AddOrUpdate(conn, stream, (c, s1) => { s1?.Dispose(); return stream; });
            return stream;
        }


        /// <summary>
        /// Create connection stream from local output endpoint to remote input endpoint
        /// </summary>
        /// <param name="localOutputEndpointName"></param>
        /// <param name="remoteProcess"></param>
        /// <param name="remoteInputEndpoint"></param>
        /// <returns></returns>
        public Stream ToRemoteInputEndpointStream(string localOutputEndpointName, string remoteProcess, string remoteInputEndpoint)
        {
            AddOutputEndpoint(localOutputEndpointName);
            _clientLibrary.AddConnectionInfo(_processName, localOutputEndpointName, remoteProcess, remoteInputEndpoint);
            var stream = Connect_InitiatorSide(_processName, localOutputEndpointName, remoteProcess, remoteInputEndpoint, false);
            var conn = new ConnectionInfo(_processName, localOutputEndpointName, remoteProcess, remoteInputEndpoint);
            ConnectionData.OutputConnections.AddOrUpdate(conn, stream, (c, s1) => { s1?.Dispose(); return stream; });
            return stream;
        }

        /// <summary>
        /// Restore information about endpoints of this detached process
        /// </summary>
        /// <returns></returns>
        public void RestoreEndpointData()
        {
            foreach (var endpt in _clientLibrary.GetInputEndpoints(_processName))
            {
                EndpointData.InputEndpoints.TryAdd(endpt, true);
            }

            foreach (var endpt in _clientLibrary.GetOutputEndpoints(_processName))
            {
                EndpointData.OutputEndpoints.TryAdd(endpt, true);
            }
        }

        /// <summary>
        /// Restore all connections from/to this process, in the CRA connection graph
        /// </summary>
        /// <returns></returns>
        public void RestoreAllConnections()
        {
            foreach (var outConn in _clientLibrary.GetConnectionsFromProcess(_processName))
            {
                var stream = ToRemoteInputEndpointStream(outConn.FromEndpoint, outConn.ToProcess, outConn.ToEndpoint);
                ConnectionData.OutputConnections.AddOrUpdate(outConn, stream, (c, s1) => { s1?.Dispose(); return stream; });
            }

            foreach (var inConn in _clientLibrary.GetConnectionsToProcess(_processName))
            {
                var stream = FromRemoteOutputEndpointStream(inConn.ToEndpoint, inConn.FromProcess, inConn.FromEndpoint);
                ConnectionData.OutputConnections.AddOrUpdate(inConn, stream, (c, s1) => { s1?.Dispose(); return stream; });
            }
        }

        /// <summary>
        /// Restore a process/instance pair
        /// </summary>
        public void Restore()
        {
            RestoreEndpointData();
            RestoreAllConnections();
        }


        /// <summary>
        /// Restore all connections from/to this process that are set to 'null' locally, in the CRA connection graph
        /// </summary>
        /// <returns></returns>
        public ConnectionData RestoreNullConnections()
        {
            foreach (var outConn in ConnectionData.OutputConnections)
            {
                if (outConn.Value == null)
                {
                    var stream = ToRemoteInputEndpointStream(outConn.Key.FromEndpoint, outConn.Key.ToProcess, outConn.Key.ToEndpoint);
                    ConnectionData.OutputConnections[outConn.Key] = stream;
                }
            }

            foreach (var inConn in ConnectionData.InputConnections)
            {
                if (inConn.Value == null)
                {
                    var stream = FromRemoteOutputEndpointStream(inConn.Key.ToEndpoint, inConn.Key.FromProcess, inConn.Key.FromEndpoint);
                    ConnectionData.InputConnections[inConn.Key] = stream;
                }
            }
            return ConnectionData;
        }


        /// <summary>
        /// Dispose the detached process
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
            
        }

        private void Dispose(bool disposed)
        {
            if (disposed)
            {
                if (_isEphemeralInstance)
                {
                    _clientLibrary.DeleteInstance(_instanceName);
                }
                _clientLibrary.DeleteProcess(_processName);
                foreach (var endpt in EndpointData.InputEndpoints.Keys)
                {
                    _clientLibrary.DeleteEndpoint(_processName, endpt);
                }
                foreach (var endpt in EndpointData.OutputEndpoints.Keys)
                {
                    _clientLibrary.DeleteEndpoint(_processName, endpt);
                }

                EndpointData.InputEndpoints.Clear();
                EndpointData.OutputEndpoints.Clear();

                foreach (var kvp in ConnectionData.InputConnections)
                {
                    _clientLibrary.DeleteConnectionInfo(kvp.Key.FromProcess, kvp.Key.FromEndpoint, kvp.Key.ToProcess, kvp.Key.ToEndpoint);
                    if (kvp.Value != null) kvp.Value.Dispose();
                }

                foreach (var kvp in ConnectionData.OutputConnections)
                {
                    _clientLibrary.DeleteConnectionInfo(kvp.Key.FromProcess, kvp.Key.FromEndpoint, kvp.Key.ToProcess, kvp.Key.ToEndpoint);
                    if (kvp.Value != null) kvp.Value.Dispose();
                }

                ConnectionData.InputConnections.Clear();
                ConnectionData.OutputConnections.Clear();
            }
        }

        private static Random random = new Random();
        private static string RandomString(int length)
        {
            const string chars = "abcdefghijklmnopqrstuvwxyz";
            return new string(Enumerable.Repeat(chars, length)
              .Select(s => s[random.Next(s.Length)]).ToArray());
        }

        private Stream Connect_InitiatorSide(string fromProcessName, string fromProcessOutput, string toProcessName, string toProcessInput, bool reverse)
        {
            bool killRemote = true; // we have no way of receiving connections

            var _processTableManager = _clientLibrary._processTableManager;

            // Need to get the latest address & port
            var row = reverse ? _processTableManager.GetRowForProcess(fromProcessName) : _processTableManager.GetRowForProcess(toProcessName);

            var _row = _processTableManager.GetRowForInstanceProcess(row.InstanceName, "");


            // Send request to CRA instance
            TcpClient client = null;
            NetworkStream ns = null;
            try
            {
                client = new TcpClient(_row.Address, _row.Port);
                client.NoDelay = true;

                ns = client.GetStream();
            }
            catch
            {
                return null;
            }

            if (!reverse)
                ns.WriteInt32((int)CRATaskMessageType.CONNECT_PROCESS_RECEIVER);
            else
                ns.WriteInt32((int)CRATaskMessageType.CONNECT_PROCESS_RECEIVER_REVERSE);

            ns.WriteByteArray(Encoding.UTF8.GetBytes(fromProcessName));
            ns.WriteByteArray(Encoding.UTF8.GetBytes(fromProcessOutput));
            ns.WriteByteArray(Encoding.UTF8.GetBytes(toProcessName));
            ns.WriteByteArray(Encoding.UTF8.GetBytes(toProcessInput));
            ns.WriteInt32(killRemote ? 1 : 0);
            CRAErrorCode result = (CRAErrorCode)ns.ReadInt32();

            if (result != 0)
            {
                Debug.WriteLine("Client received error code: " + result);
                ns.Dispose();
                return null;
            }
            else
            {
                return ns;
            }
        }
    }
}
