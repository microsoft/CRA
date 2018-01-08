using System;
using System.IO;

namespace CRA.ClientLibrary
{
    /// <summary>
    /// Describes a stream connection between two CRA entities (instances and vertices)
    /// </summary>
    public class StreamConnection : IDisposable
    {
        /// <summary>
        /// Stream connection is from this IP address
        /// </summary>
        public string FromIPAddress { get; set; }

        /// <summary>
        /// Stream connection is from this prot
        /// </summary>
        public string FromPort { get; set; }

        /// <summary>
        /// Stream connection is to this IP address
        /// </summary>
        public string ToIPAddress { get; set; }

        /// <summary>
        /// Stream connection is to this port
        /// </summary>
        public string ToPort { get; set; }

        /// <summary>
        /// Stream object for this connection
        /// </summary>
        public Stream Stream { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="fromIPAddress"></param>
        /// <param name="fromPort"></param>
        /// <param name="toIPAddress"></param>
        /// <param name="toPort"></param>
        /// <param name="stream"></param>
        public StreamConnection(string fromIPAddress, string fromPort, string toIPAddress, string toPort, Stream stream)
        {
            this.FromIPAddress = fromIPAddress;
            this.FromPort = fromPort;
            this.ToIPAddress = toIPAddress;
            this.ToPort = toPort;
            this.Stream = stream;
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="toIPAddress"></param>
        /// <param name="toPort"></param>
        /// <param name="stream"></param>
        public StreamConnection(string toIPAddress, string toPort, Stream stream)
        {
            this.FromIPAddress = "";
            this.FromPort = "";
            this.ToIPAddress = toIPAddress;
            this.ToPort = toPort;
            this.Stream = stream;
        }

        /// <summary>
        /// String representation of stream connection between two CRA entities
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return new { FromIPAddress = FromIPAddress, FromPort = FromPort, ToIPAddress = ToIPAddress, ToPort = ToPort }.ToString();
        }

        /// <summary>
        /// Check if two instances are equal
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object obj)
        {
            var otherConnectionInfo = (StreamConnection)obj;
            if (otherConnectionInfo == null) return false;

            return
                (FromIPAddress == otherConnectionInfo.FromIPAddress) &&
                (FromPort == otherConnectionInfo.FromPort) &&
                (ToIPAddress == otherConnectionInfo.ToIPAddress) &&
                (ToPort == otherConnectionInfo.ToPort);
        }

        public override int GetHashCode()
        {
            return FromIPAddress.GetHashCode() ^ FromPort.GetHashCode() ^ ToIPAddress.GetHashCode() ^ ToPort.GetHashCode();
        }

        /// <summary>
        /// Dispose actions
        /// </summary>
        public void Dispose()
        {
            Stream.Dispose();
            Stream.Close();
        }
    }
}
