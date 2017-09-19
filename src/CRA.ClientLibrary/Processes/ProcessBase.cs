using System;
using System.Collections.Concurrent;

namespace CRA.ClientLibrary
{
    /// <summary>
    /// Base class for Process abstraction
    /// </summary>
    public abstract class ProcessBase : IProcess
    {
        private string _processName;

        private ConcurrentDictionary<string, IProcessInputEndpoint> _inputEndpoints = new ConcurrentDictionary<string, IProcessInputEndpoint>();
        private ConcurrentDictionary<string, IProcessOutputEndpoint> _outputEndpoints = new ConcurrentDictionary<string, IProcessOutputEndpoint>();
        private Action<string, IProcessInputEndpoint> onAddInputEndpoint;
        private Action<string, IProcessOutputEndpoint> onAddOutputEndpoint;
        private Action onDispose;

        private ConcurrentDictionary<string, IAsyncProcessInputEndpoint> _asyncInputEndpoints = new ConcurrentDictionary<string, IAsyncProcessInputEndpoint>();
        private ConcurrentDictionary<string, IAsyncProcessOutputEndpoint> _asyncOutputEndpoints = new ConcurrentDictionary<string, IAsyncProcessOutputEndpoint>();
        private Action<string, IAsyncProcessInputEndpoint> onAddAsyncInputEndpoint;
        private Action<string, IAsyncProcessOutputEndpoint> onAddAsyncOutputEndpoint;
        private CRAClientLibrary _clientLibrary;

        /// <summary>
        /// Constructor
        /// </summary>
        protected ProcessBase()
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
        /// Dictionary of output endpoints for the process
        /// </summary>
        public ConcurrentDictionary<string, IProcessOutputEndpoint> OutputEndpoints
        {
            get
            {
                return _outputEndpoints;
            }
        }

        /// <summary>
        /// Dictionary of input endpoints for the process
        /// </summary>
        public ConcurrentDictionary<string, IProcessInputEndpoint> InputEndpoints
        {
            get
            {
                return _inputEndpoints;
            }
        }

        /// <summary>
        /// Dictionary of async output endpoints for the process
        /// </summary>
        public ConcurrentDictionary<string, IAsyncProcessOutputEndpoint> AsyncOutputEndpoints
        {
            get
            {
                return _asyncOutputEndpoints;
            }
        }

        /// <summary>
        /// Dictionary of async input endpoints for the process
        /// </summary>
        public ConcurrentDictionary<string, IAsyncProcessInputEndpoint> AsyncInputEndpoints
        {
            get
            {
                return _asyncInputEndpoints;
            }
        }

        public void ActivateProcess()
        {
            _clientLibrary.ActivateProcess(_processName);
        }

        /// <summary>
        /// Connect local output endpoint (ToStream) to remote process' input endpoint (FromStream)
        /// </summary>
        /// <param name="localOutputEndpoint">Local output endpoint</param>
        /// <param name="remoteProcess">Remote process name</param>
        /// <param name="remoteInputEndpoint">Remote input endpoint</param>
        public void ConnectLocalOutputEndpoint(string localOutputEndpoint, string remoteProcess, string remoteInputEndpoint)
        {
            _clientLibrary.Connect(_processName, localOutputEndpoint, remoteProcess, remoteInputEndpoint);
        }

        /// <summary>
        /// Connect local input endpoint (FromStream) to remote process' output endpoint (ToStream)
        /// </summary>
        /// <param name="localInputEndpoint">Local input endpoint</param>
        /// <param name="remoteProcess">Remote process name</param>
        /// <param name="remoteOutputEndpoint">Remote output endpoint</param>
        public void ConnectLocalInputEndpoint(string localInputEndpoint, string remoteProcess, string remoteOutputEndpoint)
        {
            _clientLibrary.Connect(remoteProcess, remoteOutputEndpoint, _processName, localInputEndpoint, ConnectionInitiator.ToSide);
        }


        /// <summary>
        /// Add callback for when input endpoint is added
        /// </summary>
        /// <param name="addInputCallback"></param>
        public void OnAddInputEndpoint(Action<string, IProcessInputEndpoint> addInputCallback)
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
        public void OnAddOutputEndpoint(Action<string, IProcessOutputEndpoint> addOutputCallback)
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
        public void OnAddAsyncInputEndpoint(Action<string, IAsyncProcessInputEndpoint> addInputCallback)
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
        public void OnAddAsyncOutputEndpoint(Action<string, IAsyncProcessOutputEndpoint> addOutputCallback)
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
        /// Get the name of the process
        /// </summary>
        /// <returns></returns>
        public string ProcessName
        {
            get
            {
                return _processName;
            }

            set
            {
                _processName = value;
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
        /// Process implementor uses this to add input endpoint
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        protected virtual void AddInputEndpoint(string key, IProcessInputEndpoint input)
        {
            lock (this)
            {
                onAddInputEndpoint(key, input);
            }
        }

        /// <summary>
        /// Process implementor uses this to add output endpoint
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        protected virtual void AddOutputEndpoint(string key, IProcessOutputEndpoint input)
        {
            lock (this)
            {
                onAddOutputEndpoint(key, input);
            }
        }

        /// <summary>
        /// Process implementor uses this to add async input endpoint
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        protected virtual void AddAsyncInputEndpoint(string key, IAsyncProcessInputEndpoint input)
        {
            lock (this)
            {
                onAddAsyncInputEndpoint(key, input);
            }
        }

        /// <summary>
        /// Process implementor uses this to add async output endpoint
        /// </summary>
        /// <param name="key"></param>
        /// <param name="input"></param>
        protected virtual void AddAsyncOutputEndpoint(string key, IAsyncProcessOutputEndpoint input)
        {
            lock (this)
            {
                onAddAsyncOutputEndpoint(key, input);
            }
        }


        /// <summary>
        /// Initialize process
        /// </summary>
        /// <param name="processParameter"></param>
        /// <param name="activateProcess"></param>
        public virtual void Initialize(object processParameter, bool activateProcess = true)
        {
            if (activateProcess)
                ActivateProcess();
        }

        /// <summary>
        /// Initialize process
        /// </summary>
        /// <param name="processParameter"></param>
        /// <param name="activateProcess"></param>
        public virtual void Initialize(object processParameter)
        {
            Initialize(processParameter, true);
        }

        /// <summary>
        /// Dispose the process
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Actual dispose occurs here
        /// </summary>
        /// <param name="disposing"></param>
        protected virtual void Dispose(bool disposing)
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
}
