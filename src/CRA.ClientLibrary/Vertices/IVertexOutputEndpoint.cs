using System;
using System.IO;
using System.Threading;

namespace CRA.ClientLibrary
{
    /// <summary>
    /// Interface for output endpoints in CRA
    /// </summary>
    public interface IVertexOutputEndpoint : IDisposable
    {
        /// <summary>
        /// Call to provide a stream for output to write to
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="otherVertex"></param>
        /// <param name="otherEndpoint"></param>
        /// <param name="token"></param>
        void ToStream(Stream stream, string otherVertex, string otherEndpoint, CancellationToken token);
    }

    /// <summary>
    /// Interface for output endpoints in CRA with fusable output
    /// </summary>
    public interface IFusableVertexOutputEndpoint : IVertexOutputEndpoint
    {
        /// <summary>
        /// Can this output endpoint fuse with the specified input endpoint?
        /// </summary>
        /// <param name="endpoint"></param>
        /// <param name="otherVertex"></param>
        /// <param name="otherEndpoint"></param>
        /// <returns></returns>
        bool CanFuseWith(IVertexInputEndpoint endpoint, string otherVertex, string otherEndpoint);

        /// <summary>
        /// Call to provide an input endpoint for output to write to
        /// </summary>
        /// <param name="endpoint"></param>
        /// <param name="token"></param>
        void ToInput(IVertexInputEndpoint endpoint, string otherVertex, string otherEndpoint, CancellationToken token);
    }
}
