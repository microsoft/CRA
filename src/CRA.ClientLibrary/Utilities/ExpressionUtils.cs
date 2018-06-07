using Aqua.TypeSystem;
using System.Linq.Expressions;
using System.Collections.Generic;
using System.Linq;
using System;
using RLinq = Remote.Linq.Expressions;

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
                    case System.Reflection.MemberTypes.Property:
                        value = ((System.Reflection.PropertyInfo)node.Member).GetValue(target, null);
                        break;
                    case System.Reflection.MemberTypes.Field:
                        value = ((System.Reflection.FieldInfo)node.Member).GetValue(target);
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


    /*
    internal class RemoteLinqConstantExpressionTypeCaster
    {
        private ITypeResolver _typeResolver;

        internal RemoteLinqConstantExpressionTypeCaster(ITypeResolver typeResolver = null)
        {
            _typeResolver = typeResolver ?? TypeResolver.Instance;
        }

        public RLinq.Expression Visit(RLinq.Expression expression)
        {
            if (ReferenceEquals(null, expression))
            {
                return null;
            }

            switch (expression.NodeType)
            {
                case RLinq.ExpressionType.Binary:
                    return VisitBinary((RLinq.BinaryExpression)expression);

                case RLinq.ExpressionType.Collection:
                    return VisitCollection((RLinq.CollectionExpression)expression);

                case RLinq.ExpressionType.Conditional:
                    return VisitConditional((RLinq.ConditionalExpression)expression);

                case RLinq.ExpressionType.Constant:
                    return VisitConstant((RLinq.ConstantExpression)expression);

                case RLinq.ExpressionType.Conversion:
                    return VisitConversion((RLinq.ConversionExpression)expression);

                case RLinq.ExpressionType.Parameter:
                    return expression;

                case RLinq.ExpressionType.Member:
                    return VisitMemberAccess((RLinq.MemberExpression)expression);

                case RLinq.ExpressionType.Unary:
                    return VisitUnary((RLinq.UnaryExpression)expression);

                case RLinq.ExpressionType.MethodCall:
                    return VisitMethodCall((RLinq.MethodCallExpression)expression);

                case RLinq.ExpressionType.Lambda:
                    return VisitLambda((RLinq.LambdaExpression)expression);

                case RLinq.ExpressionType.ListInit:
                    return VisitListInit((RLinq.ListInitExpression)expression);

                case RLinq.ExpressionType.New:
                    return VisitNew((RLinq.NewExpression)expression);

                case RLinq.ExpressionType.NewArray:
                    return VisitNewArray((RLinq.NewArrayExpression)expression);

                case RLinq.ExpressionType.MemberInit:
                    return VisitMemberInit((RLinq.MemberInitExpression)expression);

                default:
                    throw new Exception(string.Format("Unknown expression type: '{0}'", expression.NodeType));

            }
        }

        protected virtual RLinq.Expression VisitMemberInit(RLinq.MemberInitExpression init)
        {
            VisitNew(init.NewExpression);
            VisitBindingList(init.Bindings);
            return init;
        }

        protected List<RLinq.MemberBinding> VisitBindingList(List<RLinq.MemberBinding> original)
        {
            if (ReferenceEquals(null, original))
                return null;

            for (int i = 0, n = original.Count; i < n; i++)
                VisitBinding(original[i]);

            return original;
        }


        protected RLinq.MemberBinding VisitBinding(RLinq.MemberBinding binding)
        {
            switch (binding.BindingType)
            {
                case RLinq.MemberBindingType.Assignment:
                    return VisitMemberAssignment((RLinq.MemberAssignment)binding);

                case RLinq.MemberBindingType.MemberBinding:
                    return VisitMemberMemberBinding((RLinq.MemberMemberBinding)binding);

                case RLinq.MemberBindingType.ListBinding:
                    return VisitMemberListBinding((RLinq.MemberListBinding)binding);

                default:
                    throw new Exception(string.Format("Unhandled binding type '{0}'", binding.BindingType));
            }
        }

        protected RLinq.MemberAssignment VisitMemberAssignment(RLinq.MemberAssignment assignment)
        {
            Visit(assignment.Expression);
            return assignment;
        }

        protected RLinq.MemberMemberBinding VisitMemberMemberBinding(RLinq.MemberMemberBinding binding)
        {
            VisitBindingList(binding.Bindings.ToList());
            return binding;
        }

        protected RLinq.MemberListBinding VisitMemberListBinding(RLinq.MemberListBinding binding)
        {
            VisitElementInitializerList(binding.Initializers);
            return binding;
        }

        protected List<RLinq.ElementInit> VisitElementInitializerList(List<RLinq.ElementInit> original)
        {
            if (ReferenceEquals(null, original))
                return null;

            for (int i = 0, n = original.Count; i < n; i++)
                VisitElementInitializer(original[i]);

            return original;
        }

        protected RLinq.ElementInit VisitElementInitializer(RLinq.ElementInit initializer)
        {
            VisitExpressionList(initializer.Arguments);
            return initializer;
        }

        protected List<RLinq.Expression> VisitExpressionList(List<RLinq.Expression> original)
        {
            if (ReferenceEquals(null, original))
                return null;

            for (int i = 0, n = original.Count; i < n; i++)
                Visit(original[i]);

            return original;
        }

        protected RLinq.NewExpression VisitNew(RLinq.NewExpression newExpression)
        {
            VisitExpressionList(newExpression.Arguments);
            return newExpression;
        }

        protected RLinq.Expression VisitNewArray(RLinq.NewArrayExpression newArrayExpression)
        {
            VisitExpressionList(newArrayExpression.Expressions);
            return newArrayExpression;
        }

        protected RLinq.Expression VisitListInit(RLinq.ListInitExpression listInitExpression)
        {
            VisitNew(listInitExpression.NewExpression);
            return listInitExpression;
        }

        protected RLinq.Expression VisitBinary(RLinq.BinaryExpression expression)
        {
            Visit(expression.LeftOperand);
            Visit(expression.RightOperand);
            Visit(expression.Conversion);
            return expression;
        }

        protected virtual RLinq.Expression VisitCollection(RLinq.CollectionExpression expression)
        {
            var items = from i in expression.List
                        select new { Old = i, New = VisitConstant(i) };

            return expression;
        }

        protected RLinq.Expression VisitConditional(RLinq.ConditionalExpression expression)
        {
            Visit(expression.Test);
            Visit(expression.IfTrue);
            Visit(expression.IfFalse);
            return expression;
        }

        protected RLinq.ConstantExpression VisitConstant(RLinq.ConstantExpression expression)
        {
            var type = _typeResolver.ResolveType(expression.Type);
            expression.Value = Convert.ChangeType(expression.Value, type);
            return expression;
        }


        protected RLinq.Expression VisitConversion(RLinq.ConversionExpression expression)
        {
            Visit(expression.Operand);
            return expression;
        }

        protected RLinq.Expression VisitMemberAccess(RLinq.MemberExpression expression)
        {
            Visit(expression.Expression);
            return expression;
        }

        protected RLinq.Expression VisitUnary(RLinq.UnaryExpression expression)
        {
            Visit(expression.Operand);
            return expression;
        }

        protected RLinq.Expression VisitMethodCall(RLinq.MethodCallExpression expression)
        {
            var instance = Visit(expression.Instance);
            var argumements = from i in expression.Arguments
                              select new { Old = i, New = Visit(i) };
            return expression;
        }

        protected RLinq.Expression VisitLambda(RLinq.LambdaExpression expression)
        {
            var exp = Visit(expression.Expression);
            return expression;
        }
    }

    public static class RemoteExpressionToLinqExpressionExtensions
    {
        public static RLinq.Expression ToTypeCastedRemoteExpression(this RLinq.Expression expression)
        {
            return new RemoteLinqConstantExpressionTypeCaster().Visit(expression);
        }
    }
    */
}
