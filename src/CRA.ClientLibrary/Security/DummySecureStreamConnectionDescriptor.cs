using System;
using System.IO;

namespace CRA.ClientLibrary
{
    /// <summary>
    /// Interface to define callbacks for securing TCP connections
    /// </summary>
    public class DummySecureStreamConnectionDescriptor : ISecureStreamConnectionDescriptor
    {
        public DummySecureStreamConnectionDescriptor()
        {

        }

        public Stream CreateSecureClient(Stream stream, string serverName)
        {
            return stream;
        }

        public Stream CreateSecureServer(Stream stream)
        {
            return stream;
        }

        public void TeardownSecureClient(Stream stream)
        {
        }

        public void TeardownSecureServer(Stream stream)
        {
        }
    }
}
