//-----------------------------------------------------------------------
// <copyright file="VertexInfo.cs" company="">
//     Copyright (c) . All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

namespace CRA.ClientLibrary.DataProvider
{
    using System;
    using System.Linq.Expressions;

    /// <summary>
    /// Definition for VertexInfo
    /// </summary>
    public struct VertexInfo
    {
        public static readonly TimeSpan HeartbeatTime = TimeSpan.FromSeconds(10);

        public VertexInfo(
            string instanceName,
            string address,
            int port,
            string vertexName,
            string vertexDefinition,
            string vertexCreateAction,
            string vertexParameter,
            bool isActive,
            bool isSharded,
            string versionId = null)
        {
            this.InstanceName = instanceName;
            this.Address = address;
            this.Port = port;
            this.VertexName = vertexName;
            this.VertexDefinition = vertexDefinition;
            this.VertexCreateAction = vertexCreateAction;
            this.VertexParameter = vertexParameter;
            this.IsActive = isActive;
            this.IsSharded = isSharded;
            this.VersionId = versionId;
        }

        public static VertexInfo Create(
            string instanceName,
            string vertexName,
            string vertexDefinition,
            string address,
            int port,
            Expression<Func<IVertex>> vertexCreateAction,
            object vertexParameter,
            bool isActive,
            bool isSharded)
        {
            string vertexCreateActionStr = null;
            if (vertexCreateAction != null)
            {
                var closureEliminator = new ClosureEliminator();
                Expression vertexedUserLambdaExpression = closureEliminator.Visit(
                        vertexCreateAction);

                vertexCreateActionStr = SerializationHelper.Serialize(vertexedUserLambdaExpression);
            }

            string vertexParameterStr = SerializationHelper.SerializeObject(vertexParameter);

            return new VertexInfo(
                instanceName: instanceName,
                address: address,
                port: port,
                vertexName: vertexName,
                vertexDefinition: vertexDefinition,
                vertexCreateAction: vertexCreateActionStr,
                vertexParameter: vertexParameterStr,
                isActive: isActive,
                isSharded: isSharded);
        }

        public static VertexInfo Create(
            string instanceName,
            string vertexName,
            string vertexDefinition,
            string address,
            int port,
            Expression<Func<IShardedVertex>> vertexCreateAction,
            object vertexParameter,
            bool isActive,
            bool isSharded)
        {
            string vertexCreateActionStr = null;
            if (vertexCreateAction != null)
            {
                var closureEliminator = new ClosureEliminator();
                Expression vertexedUserLambdaExpression = closureEliminator.Visit(
                        vertexCreateAction);

                vertexCreateActionStr = SerializationHelper.Serialize(vertexedUserLambdaExpression);
            }

            string vertexParameterStr = SerializationHelper.SerializeObject(vertexParameter);

            return new VertexInfo(
                instanceName: instanceName,
                address: address,
                port: port,
                vertexName: vertexName,
                vertexDefinition: vertexDefinition,
                vertexCreateAction: vertexCreateActionStr,
                vertexParameter: vertexParameterStr,
                isActive: isActive,
                isSharded: isSharded);
        }


        public string InstanceName { get; }

        public string VertexName { get; }

        public string VertexDefinition { get; }

        public string Address { get; }

        public int Port { get; }

        public string VertexCreateAction { get; }

        public string VertexParameter { get; }

        public bool IsActive { get; }

        public bool IsSharded { get; }

        public string VersionId { get; }

        public VertexInfo Deactivate()
        {
            return new VertexInfo(
                instanceName: this.InstanceName,
                address: this.Address,
                port: this.Port,
                vertexName: this.VertexName,
                vertexDefinition: this.VertexDefinition,
                vertexCreateAction: this.VertexCreateAction,
                vertexParameter: this.VertexParameter,
                isActive: false,
                isSharded: this.IsSharded);
        }

        public VertexInfo Activate()
        {
            return new VertexInfo(
                instanceName: this.InstanceName,
                address: this.Address,
                port: this.Port,
                vertexName: this.VertexName,
                vertexDefinition: this.VertexDefinition,
                vertexCreateAction: this.VertexCreateAction,
                vertexParameter: this.VertexParameter,
                isActive: true,
                isSharded: this.IsSharded);
        }

        public override string ToString()
        {
            return string.Format(
                "Instance '{0}', Address '{1}', Port '{2}'",
                this.InstanceName,
                this.Address,
                this.Port);
        }

        public override bool Equals(object obj)
        {
            switch (obj)
            {
                case VertexInfo other:
                    return this.InstanceName == other.InstanceName
                        && this.VertexName == other.VertexName;
            }

            return false;
        }

        public override int GetHashCode()
        {
            return this.InstanceName.GetHashCode()
                + this.VertexName.GetHashCode();
        }

        public static bool operator ==(VertexInfo left, VertexInfo right)
            => left.VertexName == right.VertexName && left.InstanceName == right.InstanceName;

        public static bool operator !=(VertexInfo left, VertexInfo right)
            => !(left == right);
    }
}
