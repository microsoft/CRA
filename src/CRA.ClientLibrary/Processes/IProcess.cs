using System;
using System.Collections.Concurrent;

namespace CRA.ClientLibrary
{

    /// <summary>
    /// User provided notion of a running process
    /// </summary>
    public interface IProcess : IDisposable
    {
        /// <summary>
        /// Ingress points for a process; these are observers
        /// </summary>
        ConcurrentDictionary<string, IProcessInputEndpoint> InputEndpoints { get; }

        /// <summary>
        /// Egress points for a process; these are observables
        /// </summary>
        ConcurrentDictionary<string, IProcessOutputEndpoint> OutputEndpoints { get; }

        /// <summary>
        /// Ingress points for a process; these are observers
        /// </summary>
        ConcurrentDictionary<string, IAsyncProcessInputEndpoint> AsyncInputEndpoints { get; }

        /// <summary>
        /// Egress points for a process; these are observables
        /// </summary>
        ConcurrentDictionary<string, IAsyncProcessOutputEndpoint> AsyncOutputEndpoints { get; }


        /// <summary>
        /// Callback that process will invoke when a new input is added
        /// </summary>
        /// <param name="addInputCallback"></param>
        void OnAddInputEndpoint(Action<string, IProcessInputEndpoint> addInputCallback);

        /// <summary>
        /// Callback that process will invoke when a new output is added
        /// </summary>
        /// <param name="addOutputCallback"></param>
        void OnAddAsyncOutputEndpoint(Action<string, IAsyncProcessOutputEndpoint> addOutputCallback);

        /// <summary>
        /// Callback that process will invoke when a new input is added
        /// </summary>
        /// <param name="addInputCallback"></param>
        void OnAddAsyncInputEndpoint(Action<string, IAsyncProcessInputEndpoint> addInputCallback);

        /// <summary>
        /// Callback that process will invoke when a new output is added
        /// </summary>
        /// <param name="addOutputCallback"></param>
        void OnAddOutputEndpoint(Action<string, IProcessOutputEndpoint> addOutputCallback);

        /// <summary>
        /// Callback when process is disposed
        /// </summary>
        void OnDispose(Action disposeCallback);

        /// <summary>
        /// Gets an instance of the CRA Client Library that the process
        /// can use to communicate with the CRA runtime.
        /// </summary>
        /// <returns>Instance of CRA Client Library</returns>
        CRAClientLibrary ClientLibrary { get; }

        /// <summary>
        /// Initialize process with specified params
        /// </summary>
        /// <param name="processParameter"></param>
        void Initialize(object processParameter);
    }
}
