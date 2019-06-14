using System;
using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Threading.Tasks;

namespace CRA.ClientLibrary
{
    /// <summary>
    /// Base class for Vertex abstraction
    /// </summary>
    public abstract class VertexBase : IVertex
    {
        private string _vertexName;

        // Sync input and output endpoints of a vertex
        private ConcurrentDictionary<string, IVertexInputEndpoint> _inputEndpoints = new ConcurrentDictionary<string, IVertexInputEndpoint>();
        private ConcurrentDictionary<string, IVertexOutputEndpoint> _outputEndpoints = new ConcurrentDictionary<string, IVertexOutputEndpoint>();
        private Action<string, IVertexInputEndpoint> onAddInputEndpoint;
        private Action<string, IVertexOutputEndpoint> onAddOutputEndpoint;
        private Action onDispose;

        // Async input and output endpoints of a vertex
        private ConcurrentDictionary<string, IAsyncVertexInputEndpoint> _asyncInputEndpoints = new ConcurrentDictionary<string, IAsyncVertexInputEndpoint>();
        private ConcurrentDictionary<string, IAsyncVertexOutputEndpoint> _asyncOutputEndpoints = new ConcurrentDictionary<string, IAsyncVertexOutputEndpoint>();
        private Action<string, IAsyncVertexInputEndpoint> onAddAsyncInputEndpoint;
        private Action<string, IAsyncVertexOutputEndpoint> onAddAsyncOutputEndpoint;

        private CRAClientLibrary _clientLibrary;

        /// <summary>
        /// Constructor
        /// </summary>
        protected VertexBase()
        {
            onAddInputEndpoint = (key, proc) => _inputEndpoints.AddOrUpdate(key, proc, (str, pr) => proc);
            onAddOutputEndpoint = (key, proc) => _outputEndpoints.AddOrUpdate(key, proc, (str, pr) => proc);

            onAddAsyncInputEndpoint = (key, proc) => _asyncInputEndpoints.AddOrUpdate(key, proc, (str, pr) => proc);
            onAddAsyncOutputEndpoint = (key, proc) => _asyncOutputEndpoints.AddOrUpdate(key, proc, (str, pr) => proc);
        }

        /// <summary>
        /// Gets or sets an instance of the CRA client library
        /// </summary>
        /// <returns></returns>
        public CRAClientLibrary ClientLibrary
        {
            get
            {
                return _clientLibrary;
            }

            set
            {
                _clientLibrary = value;
            }
        }

        /// <summary>
        /// Dictionary of output endpoints for the vertex
        /// </summary>
        public ConcurrentDictionary<string, IVertexOutputEndpoint> OutputEndpoints
        {
            get
            {
                return _outputEndpoints;
            }
        }

        /// <summary>
        /// Dictionary of input endpoints for the vertex
        /// </summary>
        public ConcurrentDictionary<string, IVertexInputEndpoint> InputEndpoints
        {
            get
            {
                return _inputEndpoints;
            }
        }

        /// <summary>
        /// Dictionary of async output endpoints for the vertex
        /// </summary>
        public ConcurrentDictionary<string, IAsyncVertexOutputEndpoint> AsyncOutputEndpoints
        {
            get
            {
                return _asyncOutputEndpoints;
            }
        }

        /// <summary>
        /// Dictionary of async input endpoints for the vertex
        /// </summary>
        public ConcurrentDictionary<string, IAsyncVertexInputEndpoint> AsyncInputEndpoints
        {
            get
            {
                return _asyncInputEndpoints;
            }
        }


        /// <summary>
        /// Connect local output endpoint (ToStream) to remote vertex's input endpoint (FromStream)
        /// </summary>
        /// <param name="localOutputEndpoint">Local output endpoint</param>
        /// <param name="remoteVertex">Remote vertex name</param>
        /// <param name="remoteInputEndpoint">Remote input endpoint</param>
        public async Task ConnectLocalOutputEndpointAsync(string localOutputEndpoint, string remoteVertex, string remoteInputEndpoint)
        {
            await _clientLibrary.ConnectAsync(_vertexName, localOutputEndpoint, remoteVertex, remoteInputEndpoint);
        }

        /// <summary>
        /// Connect local input endpoint (FromStream) to remote vertex' output endpoint (ToStream)
        /// </summary>
        /// <param name="localInputEndpoint">Local input endpoint</param>
        /// <param name="remoteVertex">Remote vertex name</param>
        /// <param name="remoteOutputEndpoint">Remote output endpoint</param>
        public async Task ConnectLocalInputEndpointAsync(string localInputEndpoint, string remoteVertex, string remoteOutputEndpoint)
        {
            await _clientLibrary.ConnectAsync(remoteVertex, remoteOutputEndpoint, _vertexName, localInputEndpoint, ConnectionInitiator.ToSide);
        }


        /// <summary>
        /// Add callback for when input endpoint is added
        /// </summary>
        /// <param name="addInputCallback"></param>
        public void OnAddInputEndpoint(Action<string, IVertexInputEndpoint> addInputCallback)
        {
            lock (this)
            {
                foreach (var key in InputEndpoints.Keys)
                {
                    addInputCallback(key, InputEndpoints[key]);
                }

                onAddInputEndpoint += addInputCallback;
            }
        }

        /// <summary>
        /// Add callback for when output endpoint is added
        /// </summary>
        /// <param name="addOutputCallback"></param>
        public void OnAddOutputEndpoint(Action<string, IVertexOutputEndpoint> addOutputCallback)
        {
            lock (this)
            {
                foreach (var key in OutputEndpoints.Keys)
                {
                    addOutputCallback(key, OutputEndpoints[key]);
                }

                onAddOutputEndpoint += addOutputCallback;
            }
        }

        /// <summary>
        /// Add callback for when async input endpoint is added
        /// </summary>
        /// <param name="addInputCallback"></param>
        public void OnAddAsyncInputEndpoint(Action<string, IAsyncVertexInputEndpoint> addInputCallback)
        {
            lock (this)
            {
                foreach (var key in AsyncInputEndpoints.Keys)
                {
                    addInputCallback(key, AsyncInputEndpoints[key]);
                }

                onAddAsyncInputEndpoint += addInputCallback;
            }
        }

        /// <summary>
        /// Add callback for when async output endpoint is added
        /// </summary>
        /// <param name="addOutputCallback"></param>
        public void OnAddAsyncOutputEndpoint(Action<string, IAsyncVertexOutputEndpoint> addOutputCallback)
        {
            lock (this)
            {
                foreach (var key in AsyncOutputEndpoints.Keys)
                {
                    addOutputCallback(key, AsyncOutputEndpoints[key]);
                }

                onAddAsyncOutputEndpoint += addOutputCallback;
            }
        }

        /// <summary>
        /// Get the name of the vertex
        /// </summary>
        /// <returns></returns>
        public virtual string VertexName
        {
            get
            {
                return _vertexName;
            }

            set
            {
                _vertexName = value;
            }
        }

        /// <summary>
        /// Callback for dispose
        /// </summary>
        /// <param name="disposeCallback"></param>
        public void OnDispose(Action disposeCallback)
        {
            lock (this)
            {
                if (onDispose == null)
                    onDispose = disposeCallback;
                else
                    onDispose += disposeCallback;
            }
        }

        /// <summary>
        /// Vertex implementor uses this to add input endpoint
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        protected virtual void AddInputEndpoint(string key, IVertexInputEndpoint input)
        {
            lock (this)
            {
                onAddInputEndpoint(key, input);
            }
        }

        /// <summary>
        /// Vertex implementor uses this to add output endpoint
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        protected virtual void AddOutputEndpoint(string key, IVertexOutputEndpoint input)
        {
            lock (this)
            {
                onAddOutputEndpoint(key, input);
            }
        }

        /// <summary>
        /// Vertex implementor uses this to add async input endpoint
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        protected virtual void AddAsyncInputEndpoint(string key, IAsyncVertexInputEndpoint input)
        {
            lock (this)
            {
                onAddAsyncInputEndpoint(key, input);
            }
        }

        /// <summary>
        /// Vertex implementor uses this to add async output endpoint
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        protected virtual void AddAsyncOutputEndpoint(string key, IAsyncVertexOutputEndpoint input)
        {
            lock (this)
            {
                onAddAsyncOutputEndpoint(key, input);
            }
        }

        /// <summary>
        /// Initialize vertex
        /// </summary>
        /// <param name="vertexParameter"></param>
        public virtual Task InitializeAsync(object vertexParameter)
        {
            return Task.CompletedTask;
        }

        /// <summary>
        /// Dispose the vertex
        /// </summary>
        public virtual void Dispose()
        {
            Dispose(true);
        }

        /// <summary>
        /// Actual dispose occurs here
        /// </summary>
        /// <param name="disposing"></param>
        protected void Dispose(bool disposing)
        {
            if (disposing)
            {
                onDispose?.Invoke();

                lock (this)
                {
                    foreach (var key in OutputEndpoints.Keys)
                    {
                        OutputEndpoints[key].Dispose();
                    }
                    foreach (var key in InputEndpoints.Keys)
                    {
                        InputEndpoints[key].Dispose();
                    }
                    foreach (var key in AsyncOutputEndpoints.Keys)
                    {
                        AsyncOutputEndpoints[key].Dispose();
                    }
                    foreach (var key in AsyncInputEndpoints.Keys)
                    {
                        AsyncInputEndpoints[key].Dispose();
                    }
                }
            }
        }
    }

    /// <summary>
    /// Base class for Sharded Vertex abstraction
    /// </summary>
    public abstract class ShardedVertexBase : VertexBase, IShardedVertex
    {
        /// <summary>
        /// Get the name of the vertex
        /// </summary>
        /// <returns></returns>
        public string GetVertexName()
        {
            return base.VertexName.Split('$')[0];
        }

        /// <summary>
        /// Initialize vertex
        /// </summary>
        /// <param name="vertexParameter"></param>
        public override async Task InitializeAsync(object vertexParameter)
        {
            var par = (Tuple<int, object>)vertexParameter;
            var shardingInfo = await ClientLibrary.GetShardingInfoAsync(GetVertexName());
            await InitializeAsync(par.Item1, shardingInfo, par.Item2);
        }

        public abstract Task InitializeAsync(int shardId, ShardingInfo shardingInfo, object vertexParameter);

        public virtual void UpdateShardingInfo(ShardingInfo shardingInfo)
        {
        }
    }
}