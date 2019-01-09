//-----------------------------------------------------------------------
// <copyright file="IBlobStorageProvider.cs" company="">
//     Copyright (c) . All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

namespace CRA.ClientLibrary.DataProvider
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Threading.Tasks;

    /// <summary>
    /// Definition for IBlobStorageProvider
    /// </summary>
    public interface IBlobStorageProvider
    {
        Task Delete(string pathKey);
        Task<Stream> GetWriteStream(string pathKey);
        Task<Stream> GetReadStream(string pathKey);
    }
}
