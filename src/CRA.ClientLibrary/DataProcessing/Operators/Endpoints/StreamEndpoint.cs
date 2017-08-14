using System.IO;

namespace CRA.ClientLibrary.DataProcessing
{
   public  class StreamEndpoint : IEndpointContent
    {
        private Stream _stream;

        public Stream Stream{ get { return _stream; } }

        public StreamEndpoint(Stream stream)
        {
            _stream = stream;
        }
    }
}
