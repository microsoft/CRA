//-----------------------------------------------------------------------
// <copyright file="IEndpointInfoProvider.cs" company="">
//     Copyright (c) . All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

namespace CRA.ClientLibrary.DataProvider
{
    using System;
    using System.Collections.Generic;
    using System.Threading.Tasks;

    /// <summary>
    /// Definition for IEndpointInfoProvider
    /// </summary>
    public interface IEndpointInfoProvider
    {
        Task<IEnumerable<EndpointInfo>> GetAll();

        Task DeleteStore();
        Task<bool> ExistsEndpoint(string vertexName, string endPoint);
        Task AddEndpoint(EndpointInfo endpointInfo);
        Task DeleteEndpoint(string vertexName, string endpointName, string versionId = "*");
        Task<EndpointInfo?> GetEndpoint(string vertexName, string endpointName);
        Task<List<EndpointInfo>> GetEndpoints(string vertexName);
    }
}
