using System.Reflection;
using System.Linq.Expressions;

namespace CRA.ClientLibrary
{
    internal class ClosureEliminator : ExpressionVisitor
    {
        protected override Expression VisitMember(MemberExpression node)
        {
            if ((node.Expression != null) && (node.Expression.NodeType == ExpressionType.Constant))
            {
                object target = ((ConstantExpression)node.Expression).Value, value;
                switch (node.Member.MemberType)
                {
                    case MemberTypes.Property:
                        value = ((PropertyInfo)node.Member).GetValue(target, null);
                        break;
                    case MemberTypes.Field:
                        value = ((FieldInfo)node.Member).GetValue(target);
                        break;
                    default:
                        value = target = null;
                        break;
                }
                if (target != null)
                {
                    if (value.GetType().IsSubclassOf(typeof(Expression)))
                    {
                        return this.Visit(value as Expression);
                    }
                    else
                    {
                        return Expression.Constant(value, node.Type);
                    }
                }
            }
            return base.VisitMember(node);
        }
    }
}
