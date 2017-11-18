using System;
using System.Runtime.Serialization;

namespace CRA.ClientLibrary
{
    /// <summary>
    /// Describes a connection between two vertex/endpoint pairs
    /// </summary>
    [Serializable, DataContract]
    public class ConnectionInfo
    {
        /// <summary>
        /// Connection is from this vertex
        /// </summary>
        [DataMember]
        public string FromVertex { get; set; }
        /// <summary>
        /// Connection is from this output endpoint
        /// </summary>

        [DataMember]

        public string FromEndpoint { get; set; }
        /// <summary>
        /// Connection is to this vertex
        /// </summary>

        [DataMember]
        public string ToVertex { get; set; }

        /// <summary>
        /// Connection is to this input endpoint
        /// </summary>
        [DataMember]
        public string ToEndpoint { get; set; }


        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="fromVertex"></param>
        /// <param name="fromEndpoint"></param>
        /// <param name="toVertex"></param>
        /// <param name="toEndpoint"></param>
        public ConnectionInfo(string fromVertex, string fromEndpoint, string toVertex, string toEndpoint)
        {
            this.FromVertex = fromVertex;
            this.FromEndpoint = fromEndpoint;
            this.ToVertex = toVertex;
            this.ToEndpoint = toEndpoint;
        }

        /// <summary>
        /// String representation of a CRA conection
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return new { FromVertex = FromVertex, FromEndpoint = FromEndpoint, ToVertex = ToVertex, ToEndpoint = ToEndpoint }.ToString();
        }

        /// <summary>
        /// Check if two instances are equal
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object obj)
        {
            var otherConnectionInfo = (ConnectionInfo)obj;
            if (otherConnectionInfo == null) return false;

            return
                (FromVertex == otherConnectionInfo.FromVertex) &&
                (ToVertex == otherConnectionInfo.ToVertex) &&
                (FromEndpoint == otherConnectionInfo.FromEndpoint) &&
                (ToEndpoint == otherConnectionInfo.ToEndpoint);
        }

        public override int GetHashCode()
        {
            return FromVertex.GetHashCode() ^ FromEndpoint.GetHashCode() ^ ToVertex.GetHashCode() ^ ToEndpoint.GetHashCode();
        }
    }

    [Serializable, DataContract]
    public class ConnectionInfoWithLocality : ConnectionInfo
    {
        [DataMember]
        public bool IsOnSameCRAInstance { get; set; }

        public ConnectionInfoWithLocality(string fromVertex, string fromEndpoint, string toVertex, string toEndpoint, bool isOnSameCRAInstance) : base(fromVertex, fromEndpoint, toVertex, toEndpoint)
        {
            IsOnSameCRAInstance = isOnSameCRAInstance;
        }
    }

}
