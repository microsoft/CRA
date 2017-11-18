using System;
using System.Collections.Concurrent;
using System.Net.Sockets;


namespace CRA.ClientLibrary
{
    /// <summary>
    /// Pooling utilities for streams in a Common Runtime for Applications (CRA) client
    /// </summary>
    public partial class CRAClientLibrary : IDisposable
    {

        // A pool of stream connections from this CRA client to other CRA running entities (instances and vertexes)
        ConcurrentDictionary<string, StreamConnectionPool> _streamConnectionPools = new ConcurrentDictionary<string, StreamConnectionPool>();

        /// <summary>
        /// Pool of stream connections
        /// </summary>
        internal ConcurrentDictionary<string, StreamConnectionPool> StreamConnectionPools { get { return _streamConnectionPools; } }

        /// <summary>
        /// Retrieve a stream connection from a pool of available connections
        /// </summary>
        /// <param name="address"> IPAddress of the machine that this CRA client wants to connect to</param>
        /// <param name="port"> Port number of the CRA entity that this CRA client wants to connect to</param>
        /// <param name="stream"> A network sender stream that connects this CRA client to the other CRA entity</param>
        /// <returns>A boolean indicates whether the retrieval operation is successful or not</returns>
        internal bool TryGetSenderStreamFromPool(string address, string port, out NetworkStream stream)
        {
            StreamConnectionPool connectionsPool;
            StreamConnection streamConnection;

            if (StreamConnectionPools.TryGetValue(address + ":" + port, out connectionsPool) &&
                    connectionsPool.Get(out streamConnection))
            {
                stream = (NetworkStream)streamConnection.Stream;
                stream.WriteInt32((int)CRATaskMessageType.PING);

                CRAErrorCode result = (CRAErrorCode)stream.ReadInt32();
                if (result != 0)
                {
                    return false;
                }

                return true;
            }
            else
            {
                stream = null;
                return false;
            }
        }

        /// <summary>
        /// Add/Return a stream connection to a pool of connections
        /// </summary>
        /// <param name="address"> IPAddress of the machine that this CRA client wants to connect to</param>
        /// <param name="port"> Port number of the CRA entity that this CRA client wants to connect to</param>
        /// <param name="stream"> A network sender stream that connects this CRA client to the other CRA entity</param>
        /// <returns>A boolean indicates whether the return operation is successful or not</returns>
        internal bool TryAddSenderStreamToPool(string address, string port, NetworkStream stream)
        {
            StreamConnectionPool connectionsPool;

            bool isPoolExist = true;
            if (!StreamConnectionPools.TryGetValue(address + ":" + port, out connectionsPool))
            {
                if (_localWorker != null)
                {
                    connectionsPool = new StreamConnectionPool(_localWorker.StreamsPoolSize, true);
                }
                else
                {
                    connectionsPool = new StreamConnectionPool(true);
                }

                isPoolExist = false;
            }

            if (connectionsPool.Return(new StreamConnection(address, port, stream)))
            {
                if (isPoolExist)
                {
                    StreamConnectionPools[address + ":" + port] = connectionsPool;
                }
                else
                {
                    if (!StreamConnectionPools.TryAdd(address + ":" + port, connectionsPool))
                    {
                        return false;
                    }
                }
            }
            else
            {
                return false;
            }

            return true;
        }

        public void Dispose()
        {
            foreach (string key in _streamConnectionPools.Keys)
            {
                _streamConnectionPools[key].Free(true);
            }
        }
    }
}
