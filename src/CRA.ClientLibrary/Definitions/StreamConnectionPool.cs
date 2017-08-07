using System.Collections.Concurrent;
using System.Threading;

namespace CRA.ClientLibrary
{
    /// <summary>
    /// A class represents a pool of stream connections in a CRA client
    /// </summary>
    internal class StreamConnectionPool
    {
        const int DEFAULT_CONNECTION_POOL_SIZE = 1000;

        private ConcurrentQueue<StreamConnection> _queue;
        private long _createdObjects;
        private int _size;
        private bool _isLimitedPool;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="isLimitedPool"></param>
        public StreamConnectionPool(bool isLimitedPool = false) : this(0, isLimitedPool)
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="inputSize"></param>
        /// <param name="isLimitedPool"></param>
        public StreamConnectionPool(int inputSize, bool isLimitedPool = false)
        {
            _queue = new ConcurrentQueue<StreamConnection>();

            if (inputSize <= 0)
            {
                _size = DEFAULT_CONNECTION_POOL_SIZE;
            }
            else
            {
                _size = inputSize;
            }

            _isLimitedPool = isLimitedPool;
            Interlocked.Exchange(ref _createdObjects, 0);
        }

        /// <summary>
        /// Adds/Returns a stream connection to the pool of connections
        /// </summary>
        /// <param name="item"></param>
        /// <returns></returns>
        public bool Return(StreamConnection item)
        {
            if (_isLimitedPool && (_createdObjects > _size))
            {
                return false;
            }

            _queue.Enqueue(item);
            Interlocked.Increment(ref _createdObjects);

            return true;
        }

        /// <summary>
        /// Retrieves a stream connection from the pool of connections
        /// </summary>
        /// <param name="result"></param>
        /// <returns></returns>
        public bool Get(out StreamConnection result)
        {
            if (!_queue.TryDequeue(out result))
            {
                return false;
            }

            Interlocked.Decrement(ref _createdObjects);
            return true;
        }

        /// <summary>
        /// Frees the pool of stream connections
        /// </summary>
        /// <param name="reset"></param>
        public void Free(bool reset = false)
        {
            StreamConnection result;
            while (_queue.TryDequeue(out result))
            {
                result.Dispose();
                Interlocked.Decrement(ref _createdObjects);
            }
            if (reset)
            {
                Interlocked.Exchange(ref _createdObjects, 0);
            }
        }
    }
}
