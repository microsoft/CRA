using System;
using System.IO;
using System.Threading;

namespace CRA.ClientLibrary
{
    /// <summary>
    /// Interface for output endpoints in CRA
    /// </summary>
    public interface IProcessOutputEndpoint : IDisposable
    {
        /// <summary>
        /// Call to provide a stream for output to write to
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="otherProcess"></param>
        /// <param name="otherEndpoint"></param>
        /// <param name="token"></param>
        void ToStream(Stream stream, string otherProcess, string otherEndpoint, CancellationToken token);

        /// <summary>
        /// Call to provide an input endpoint for output to write to
        /// </summary>
        /// <param name="endpoint"></param>
        /// <param name="token"></param>
        void ToInput(IProcessInputEndpoint endpoint, CancellationToken token);
    }
}
