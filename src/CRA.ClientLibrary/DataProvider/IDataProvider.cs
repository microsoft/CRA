namespace CRA.DataProvider
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Definition for IDataProvider
    /// </summary>
    public interface IDataProvider
    {
        IBlobStorageProvider GetBlobStorageProvider();
        IVertexInfoProvider GetVertexInfoProvider();
        IEndpointInfoProvider GetEndpointInfoProvider();
        IShardedVertexInfoProvider GetShardedVertexInfoProvider();
        IVertexConnectionInfoProvider GetVertexConnectionInfoProvider();
    }
}
