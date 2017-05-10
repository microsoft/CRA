using System.Reflection;
using System.Collections.Generic;
using System.Diagnostics.Contracts;

namespace CRA.ClientLibrary
{
    internal static class AssemblyResolverClient
    {
        internal struct ApplicationAssembly
        {
            public string Name;

            public string FileName;
        }

        private static void GetApplicationAssemblies(
            AssemblyName current, List<ApplicationAssembly> assemblies, HashSet<string> exclude)
        {
            Contract.Requires(current != null);
            Contract.Requires(assemblies != null);
            Contract.Requires(exclude != null);

            // If this assembly is in the "exclude" set, then done.
            var name = current.FullName;
            if (exclude.Contains(name))
            {
                return;
            }

            // Add to "exclude" set so this assembly isn't re-added if it is referenced additional times.
            exclude.Add(name);

            // Recursively add any assemblies that "current" references.
            var assembly = Assembly.Load(current);
            foreach (var referenced in assembly.GetReferencedAssemblies())
            {
                GetApplicationAssemblies(referenced, assemblies, exclude);
            }

            // Lastly, now that all dependant (referenced) assemblies are added, add the "current" assembly.
            assemblies.Add(new ApplicationAssembly { Name = name, FileName = assembly.Location });
        }

    }
}
