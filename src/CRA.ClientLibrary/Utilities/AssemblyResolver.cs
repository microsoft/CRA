using System;
using System.Threading;
using System.Reflection;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.Contracts;

#pragma warning disable 420

namespace CRA.ClientLibrary
{
    internal static class AssemblyResolver
    {
        private static readonly ConcurrentDictionary<string, byte[]> assemblies =
            new ConcurrentDictionary<string, byte[]>();

        private static volatile int handlerRegistered;

        public static IEnumerable<string> RegisteredAssemblies
        {
            get
            {
                return assemblies.Keys;
            }
        }

        public static bool ContainsAssembly(string name)
        {
            Contract.Requires(name != null);
            return assemblies.ContainsKey(name);
        }

        public static byte[] GetAssemblyBytes(string name)
        {
            Contract.Requires(name != null);
            return assemblies[name];
        }

        public static void Register(string name, byte[] assembly)
        {
            Contract.Requires(name != null);
            Contract.Requires(assembly != null);
            assemblies.TryAdd(name, assembly);
            if (handlerRegistered == 0 &&
                Interlocked.CompareExchange(ref handlerRegistered, 1, 0) == 0)
            {
                AppDomain.CurrentDomain.AssemblyResolve += Resolver;
            }
        }

        private static Assembly Resolver(object sender, ResolveEventArgs arguments)
        {
            Assembly assembly = null;
            byte[] assemblyBytes;
            if (assemblies.TryGetValue(arguments.Name, out assemblyBytes))
            {
                if (IsAssemblyLoaded(arguments.Name, out assembly))
                    return assembly;
                else
                    return Assembly.Load(assemblyBytes);
            }
            return assembly;
        }

        private static bool IsAssemblyLoaded(string fullName, out Assembly lAssembly)
        {
            Assembly[] assemblies = AppDomain.CurrentDomain.GetAssemblies();

            foreach (Assembly assembly in assemblies)
            {
                if (assembly.FullName == fullName)
                {
                    lAssembly = assembly;
                    return true;
                }
            }

            lAssembly = null;
            return false;
        }

    }
}
