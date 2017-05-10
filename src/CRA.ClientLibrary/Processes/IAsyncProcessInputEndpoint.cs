using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace CRA.ClientLibrary
{
    /// <summary>
    /// Interface for async input endpoints in CRA
    /// </summary>
    public interface IAsyncProcessInputEndpoint : IDisposable
    {
        /// <summary>
        /// Async version of FromStream
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="otherProcess"></param>
        /// <param name="otherEndpoint"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        Task FromStreamAsync(Stream stream, string otherProcess, string otherEndpoint, CancellationToken token);

        /// <summary>
        /// Async version of FromOutput
        /// </summary>
        /// <param name="endpoint"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        Task FromOutputAsync(IProcessOutputEndpoint endpoint, CancellationToken token);
    }
}
