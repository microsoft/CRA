using System;
using System.IO;
using System.Threading;

namespace CRA.ClientLibrary
{
    /// <summary>
    /// Interface for input endpoints in CRA
    /// </summary>
    public interface IProcessInputEndpoint : IDisposable
    {
        /// <summary>
        /// Call to provide a stream for input to read from
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="otherProcess"></param>
        /// <param name="otherEndpoint"></param>
        /// <param name="token"></param>
        void FromStream(Stream stream, string otherProcess, string otherEndpoint, CancellationToken token);

        /// <summary>
        /// Call to provide an output endpoint for input to read from
        /// </summary>
        /// <param name="endpoint"></param>
        /// <param name="token"></param>
        void FromOutput(IProcessOutputEndpoint endpoint, CancellationToken token);
    }
}
