namespace CRA.DataProvider
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;

    /// <summary>
    /// Definition for VertexConnectionInfo
    /// </summary>
    public struct VertexConnectionInfo
    {
        public VertexConnectionInfo(
            string fromVertex,
            string fromEndpoint,
            string toVertex,
            string toEndpoint,
            string versionId = null)
        {
            FromVertex = fromVertex;
            FromEndpoint = fromEndpoint;
            ToVertex = toVertex;
            ToEndpoint = toEndpoint;
            VersionId = versionId;
        }

        public string FromVertex { get; }

        public string FromEndpoint { get; }

        public string ToVertex { get; }

        public string ToEndpoint { get; }

        public string VersionId { get; }

        public override string ToString()
        {
            return string.Format(
                CultureInfo.CurrentCulture,
                "FromVertex '{0}', FromEndpoint '{1}', ToVertex '{2}', ToEndpoint '{3}'",
                FromVertex,
                FromEndpoint,
                ToVertex,
                ToEndpoint);
        }

        public override bool Equals(object obj)
        {
            switch (obj)
            {
                case VertexConnectionInfo other:
                    return this == other;
            }

            return false;
        }

        public override int GetHashCode()
        {
            return 
                this.FromVertex.GetHashCode()
                ^ (this.FromEndpoint.GetHashCode() << 1)
                ^ (this.ToVertex.GetHashCode()) << 2
                ^ (this.ToEndpoint.GetHashCode() << 3);
        }

        public static bool operator ==(
            VertexConnectionInfo left,
            VertexConnectionInfo right)
        {
            return left.FromEndpoint == right.FromEndpoint
                && left.FromVertex == right.FromVertex
                && left.ToEndpoint == right.ToEndpoint
                && left.ToVertex == right.ToVertex;
        }

        public static bool operator !=(
            VertexConnectionInfo left,
            VertexConnectionInfo right)
        {
            return !(left == right);
        }
    }
}
