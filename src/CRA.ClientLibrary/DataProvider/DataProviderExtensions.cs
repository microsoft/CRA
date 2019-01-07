//-----------------------------------------------------------------------
// <copyright file="DataProviderExtensions.cs" company="">
//     Copyright (c) . All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

namespace CRA.ClientLibrary.DataProvider
{
    using System;
    using System.Collections.Generic;
    using System.Linq.Expressions;

    /// <summary>
    /// Definition for DataProviderExtensions
    /// </summary>
    public static class DataProviderExtensions
    {
        internal static Func<IVertex> GetVertexCreateAction(
            this VertexInfo vertexInfo)
        {
            var expr = SerializationHelper.Deserialize(vertexInfo.VertexCreateAction);
            var actionExpr = AddBox((LambdaExpression)expr);
            return actionExpr.Compile();
        }

        private static Expression<Func<IVertex>> AddBox(LambdaExpression expression)
        {
            Expression converted = Expression.Convert
                 (expression.Body, typeof(IVertex));
            return Expression.Lambda<Func<IVertex>>
                 (converted, expression.Parameters);
        }
    }
}
