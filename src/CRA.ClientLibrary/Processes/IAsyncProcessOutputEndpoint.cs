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
    }


    /// <summary>
    /// Interface for async output endpoints in CRA with fusable output
    /// </summary>
    public interface IAsyncFusableProcessOutputEndpoint : IAsyncProcessOutputEndpoint
    {
        /// <summary>
        /// Can this output endpoint fuse with the specified input endpoint?
        /// </summary>
        /// <param name="endpoint"></param>
        /// <param name="otherProcess"></param>
        /// <param name="otherEndpoint"></param>
        /// <returns></returns>
        bool CanFuseWith(IAsyncProcessInputEndpoint endpoint, string otherProcess, string otherEndpoint);

        /// <summary>
        /// Async version of ToInput
        /// </summary>
        /// <param name="endpoint"></param>
        /// <param name="token"></param>
        /// <returns></returns>
        Task ToInputAsync(IAsyncProcessInputEndpoint endpoint, string otherProcess, string otherEndpoint, CancellationToken token);
    }
}
