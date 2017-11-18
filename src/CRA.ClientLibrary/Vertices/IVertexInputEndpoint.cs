using System;
using System.IO;
using System.Threading;

namespace CRA.ClientLibrary
{
    /// <summary>
    /// Interface for input endpoints in CRA
    /// </summary>
    public interface IVertexInputEndpoint : IDisposable
    {
        /// <summary>
        /// Call to provide a stream for input to read from
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="otherVertex"></param>
        /// <param name="otherEndpoint"></param>
        /// <param name="token"></param>
        void FromStream(Stream stream, string otherVertex, string otherEndpoint, CancellationToken token);
    }
}
