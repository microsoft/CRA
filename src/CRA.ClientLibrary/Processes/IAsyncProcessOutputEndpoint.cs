using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace CRA.ClientLibrary
{
    /// <summary>
    /// Interface for async output endpoints in CRA
    /// </summary>
    public interface IAsyncProcessOutputEndpoint : IDisposable
    {
        /// <summary>
        /// Async version of ToStream
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="otherProcess"></param>
        /// <param name="otherEndpoint"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        Task ToStreamAsync(Stream stream, string otherProcess, string otherEndpoint, CancellationToken token);

        /// <summary>
        /// Async version of ToInput
        /// </summary>
        /// <param name="endpoint"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        Task ToInputAsync(IProcessInputEndpoint endpoint, CancellationToken token);
    }
}
