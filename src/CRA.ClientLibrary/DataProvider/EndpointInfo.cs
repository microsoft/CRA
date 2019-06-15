namespace CRA.DataProvider
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Definition for EndpointInfo
    /// </summary>
    public struct EndpointInfo
    {
        /// <summary>
        /// The time interval at which workers refresh their membership entry
        /// </summary>
        public static readonly TimeSpan HeartbeatTime = TimeSpan.FromSeconds(10);

        public EndpointInfo(
            string vertexName,
            string endpointName,
            bool isInput,
            bool isAsync,
            string versionId = null)
        {
            this.VertexName = vertexName;
            this.EndpointName = endpointName;
            this.IsInput = isInput;
            this.IsAsync = isAsync;
            this.VersionId = versionId;
        }

        /// <summary>
        /// Name of the group
        /// </summary>
        public string VertexName { get; }

        /// <summary>
        /// Endpoint name
        /// </summary>
        public string EndpointName { get; }

        /// <summary>
        /// Is an input (or output)
        /// </summary>
        public bool IsInput { get; }

        /// <summary>
        /// Is async (or sync)
        /// </summary>
        public bool IsAsync { get; }

        /// <summary>
        /// Version Id
        /// </summary>
        public string VersionId { get; }

        public override string ToString()
            => string.Format("Vertex '{0}', Endpoint '{1}'", VertexName, EndpointName);

        /// <summary>
        /// Equals
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object obj)
        {
            switch (obj)
            {
                case EndpointInfo ei:
                    return this == ei;
            }

            return false;
        }

        /// <summary>
        /// GetHashCode
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            if (this.VertexName == null || this.EndpointName == null)
            { return 0; }

            return this.VertexName.GetHashCode() ^ this.EndpointName.GetHashCode();
        }

        public static bool operator ==(EndpointInfo left, EndpointInfo right)
        {
            return left.VertexName == right.VertexName && left.EndpointName == right.EndpointName;
        }

        public static bool operator !=(EndpointInfo left, EndpointInfo right)
        {
            return !(left == right);
        }
    }
}
