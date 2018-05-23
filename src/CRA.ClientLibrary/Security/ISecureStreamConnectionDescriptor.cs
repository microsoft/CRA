using System;
using System.IO;

namespace CRA.ClientLibrary
{
    /// <summary>
    /// Interface to define callbacks for securing TCP connections
    /// </summary>
    public interface ISecureStreamConnectionDescriptor
    {
        Stream CreateSecureClient(Stream stream, string serverName);
        Stream CreateSecureServer(Stream stream);
        void TeardownSecureClient(Stream stream);
        void TeardownSecureServer(Stream stream);
    }
}
