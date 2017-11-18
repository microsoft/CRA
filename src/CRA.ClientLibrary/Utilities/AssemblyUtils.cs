using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Globalization;
using System.Runtime.Serialization;

namespace CRA.ClientLibrary
{
    internal static class AssemblyUtils
    {
        internal struct ApplicationAssembly
        {
            public string Name;

            public string FilePath;

            public bool ManagedReference;
        }

        [DataContract]
        internal struct UserDLLsInfo
        {
            [DataMember]
            public string UserDLLsBuffer { get; set; }

            [DataMember]
            public string UserDLLsBufferInfo { get; set; }
        }


        public static string AssemblyDirectory
        {
            get
            {
                string codeBase = Assembly.GetExecutingAssembly().CodeBase;
                UriBuilder uri = new UriBuilder(codeBase);
                string path = Uri.UnescapeDataString(uri.Path);
                return Path.GetDirectoryName(path);
            }
        }

        public static string[] GetExcludedAssemblies()
        {
            string[] excludedAssemblies = new[]
            {
                "mscorlib, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089",
                "System, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089",
                "System.Core, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089",
                "Microsoft.Azure.KeyVault.Core, Version=1.0.0.0, Culture=neutral, PublicKeyToken=31bf3856ad364e35",
                "Microsoft.VisualStudio.HostingVertex.Utilities, Version=14.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "mscorlib, Version=2.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089",
                "System, Version=2.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089",
                "System.Configuration, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Xml, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089",
                "System.Data.SqlXml, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089",
                "System.Security, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Windows.Forms, Version=2.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089",
                "System.Drawing, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "Accessibility, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Deployment, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Windows.Forms, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089",
                "System.Runtime.Serialization.Formatters.Soap, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "Microsoft.VisualStudio.HostingVertex.Utilities.Sync, Version=14.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "Microsoft.VisualStudio.Debugger.Runtime, Version=14.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "vshost32, Version=14.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Xml.Linq, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089",
                "System.Runtime.Serialization, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089",
                "System.ServiceModel.Internals, Version=4.0.0.0, Culture=neutral, PublicKeyToken=31bf3856ad364e35",
                "SMDiagnostics, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089",
                "System.Data.DataSetExtensions, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089",
                "System.Data, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089",
                "System.Transactions, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089",
                "System.EnterpriseServices, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.DirectoryServices, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Runtime.Remoting, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089",
                "System.Web, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Web.RegularExpressions, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Design, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Data.OracleClient, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089",
                "System.Drawing.Design, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Web.ApplicationServices, Version=4.0.0.0, Culture=neutral, PublicKeyToken=31bf3856ad364e35",
                "System.ComponentModel.DataAnnotations, Version=4.0.0.0, Culture=neutral, PublicKeyToken=31bf3856ad364e35",
                "System.DirectoryServices.Protocols, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Runtime.Caching, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.ServiceVertex, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Configuration.Install, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Web.Services, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "Microsoft.Build.Utilities.v4.0, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "Microsoft.Build.Framework, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Xaml, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089",
                "Microsoft.Build.Tasks.v4.0, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Numerics, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089",
                "Microsoft.CSharp, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Dynamic, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Net.Http, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "Microsoft.WindowsAzure.Storage, Version=8.1.1.0, Culture=neutral, PublicKeyToken=31bf3856ad364e35",
                "Microsoft.Data.Services.Client, Version=5.8.1.0, Culture=neutral, PublicKeyToken=31bf3856ad364e35",
                "Microsoft.Data.Edm, Version=5.8.1.0, Culture=neutral, PublicKeyToken=31bf3856ad364e35",
                "Microsoft.Data.OData, Version=5.8.1.0, Culture=neutral, PublicKeyToken=31bf3856ad364e35",
                "System.Spatial, Version=5.8.1.0, Culture=neutral, PublicKeyToken=31bf3856ad364e35",
                "Newtonsoft.Json, Version=6.0.0.0, Culture=neutral, PublicKeyToken=30ad4fe6b2a6aeed",
                "Newtonsoft.Json, Version=8.0.0.0, Culture=neutral, PublicKeyToken=30ad4fe6b2a6aeed",
                "Newtonsoft.Json, Version=10.0.0.0, Culture=neutral, PublicKeyToken=30ad4fe6b2a6aeed",

                "Remote.Linq, Version=5.1.0.0, Culture=neutral, PublicKeyToken=null",
                "Aqua, Version=2.0.0.0, Culture=neutral, PublicKeyToken=null",
                "Anonymously Hosted DynamicMethods Assembly, Version=0.0.0.0, Culture=neutral, PublicKeyToken=null",
                "Remote.Linq, Version=5.3.1.0, Culture=neutral, PublicKeyToken=null",
                "Aqua, Version=3.0.0.0, Culture=neutral, PublicKeyToken=null",
                "Aqua.TypeSystem.Emit.Types, Version=0.0.0.0, Culture=neutral, PublicKeyToken=null",
                "Remote.Linq, Version=5.4.0.0, Culture=neutral, PublicKeyToken=null",
                "Aqua, Version=4.0.0.0, Culture=neutral, PublicKeyToken=null",

                "System.ServiceModel, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089",
                "System.IdentityModel, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089",
                "Microsoft.Transactions.Bridge, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.IdentityModel.Selectors, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089",
                "System.Messaging, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Runtime.DurableInstancing, Version=4.0.0.0, Culture=neutral, PublicKeyToken=31bf3856ad364e35",
                "System.ServiceModel.Activation, Version=4.0.0.0, Culture=neutral, PublicKeyToken=31bf3856ad364e35",
                "System.ServiceModel.Activities, Version=4.0.0.0, Culture=neutral, PublicKeyToken=31bf3856ad364e35",
                "System.Activities, Version=4.0.0.0, Culture=neutral, PublicKeyToken=31bf3856ad364e35",
                "Microsoft.VisualBasic.Activities.Compiler, Version=10.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "Microsoft.VisualBasic, Version=10.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Management, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "Microsoft.JScript, Version=10.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Activities.DurableInstancing, Version=4.0.0.0, Culture=neutral, PublicKeyToken=31bf3856ad364e35",
                "System.Xaml.Hosting, Version=4.0.0.0, Culture=neutral, PublicKeyToken=31bf3856ad364e35",
                "CRA.ClientLibrary, Version=1.0.0.0, Culture=neutral, PublicKeyToken=null",
            };
            return excludedAssemblies;
        }


        public static ApplicationAssembly[] GetRelatedApplicationAssemblies(
            string assemblyNamePrefix, params string[] excludedAssemblies)
        {
            Contract.Requires(excludedAssemblies != null);

            var assemblies = new List<ApplicationAssembly>();
            var exclude = new HashSet<string>(excludedAssemblies);

            var applicationAssemblies = AppDomain.CurrentDomain.GetAssemblies();
            foreach (var applicationAssembly in applicationAssemblies
                                                .Where(v => v.GetName().FullName.StartsWith(
                                                    assemblyNamePrefix)))
            {
                GetApplicationAssemblies(applicationAssembly.GetName(), assemblies, exclude);
            }

            return assemblies.ToArray();
        }

        private static void GetApplicationAssemblies(AssemblyName current,
            List<ApplicationAssembly> assemblies, HashSet<string> exclude)
        {
            Contract.Requires(current != null);
            Contract.Requires(assemblies != null);
            Contract.Requires(exclude != null);

            var name = current.FullName;
            if (exclude.Contains(name))
            {
                return;
            }

            exclude.Add(name);

            try
            {
                var assembly = Assembly.Load(current);
                foreach (var referenced in assembly.GetReferencedAssemblies())
                {
                    GetApplicationAssemblies(referenced, assemblies, exclude);
                }

                assemblies.Add(new ApplicationAssembly { Name = name, FilePath = assembly.Location, ManagedReference = true });

                // Now if the assembly has a location, look to see if there are any other
                // dlls that need to get carried along
                var assemblyPath = assembly.Location;
                if (!String.IsNullOrWhiteSpace(assemblyPath))
                {
                    // ignore it if it is a framework assembly?
                    var dlls = Directory.GetFiles(Path.GetDirectoryName(assemblyPath), "*.dll", SearchOption.TopDirectoryOnly);
                    foreach (var dllPath in dlls)
                    {
                        var dllName = Path.GetFileNameWithoutExtension(dllPath);
                        if (dllName.Equals(assembly.GetName().Name)) continue;
                        if (assemblies.Any(e => e.Name.StartsWith(dllName))) continue;
                        if (exclude.Any(e => e.StartsWith(dllName))) continue;
                        assemblies.Add(new ApplicationAssembly { Name = dllName, FilePath = dllPath, ManagedReference = false, });
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
        }

        public static Dictionary<string, byte[]> AssembliesFromString(string assembliesString,
                                                                      string assembliesStringInfo)
        {
            Dictionary<string, byte[]> udfAssemblies = new Dictionary<string, byte[]>();

            if (!assembliesStringInfo.Equals("$"))
            {
                string[] parts = assembliesStringInfo.Split('@');

                MemoryStream assembliesStream = new MemoryStream(
                                            Convert.FromBase64String(assembliesString));
                BinaryReader binaryReader = new BinaryReader(assembliesStream);
                for (int i = 0; i < parts.Length - 1; i = i + 2)
                {
                    int assemblyNameSize = int.Parse(parts[i], CultureInfo.InvariantCulture);
                    int assemblyBytesSize = int.Parse(parts[i + 1], CultureInfo.InvariantCulture);
                    string assemblyName = Encoding.UTF8.GetString(
                        binaryReader.ReadBytes(assemblyNameSize));
                    byte[] assemblyBytes = binaryReader.ReadBytes(assemblyBytesSize);
                    udfAssemblies.Add(assemblyName, assemblyBytes);
                }
            }

            return udfAssemblies;
        }

        public static void WriteAssembliesToStream(Stream stream)
        {
            var relatedAssemblies = GetRelatedApplicationAssemblies("",
                                                                    GetExcludedAssemblies());

            stream.WriteInt32(relatedAssemblies.Length);

            foreach (var assembly in relatedAssemblies)
            {
                byte[] assemblyNameBytes = Encoding.UTF8.GetBytes(assembly.Name);
                stream.WriteByteArray(assemblyNameBytes);

                stream.WriteByte(assembly.ManagedReference ? (byte)1 : (byte)0);

                FileStream fs = new FileStream(assembly.FilePath, FileMode.Open, FileAccess.Read);

                using (BinaryReader br = new BinaryReader(fs))
                {
                    byte[] assemblyFileBytes = br.ReadBytes(Convert.ToInt32(fs.Length));
                    stream.WriteByteArray(assemblyFileBytes);
                }
            }
        }

        public static void LoadAssembliesFromStream(Stream stream)
        {
            int numAssemblies = stream.ReadInt32();

            for (int i = 0; i < numAssemblies; i++)
            {
                byte[] assemblyNameBytes = stream.ReadByteArray();
                string assemblyName = Encoding.UTF8.GetString(assemblyNameBytes);

                var managedAssembly = stream.ReadByte() == 0 ? false : true;

                byte[] assemblyFileBytes = stream.ReadByteArray();

                if (!managedAssembly)
                {
                    var assemblyPath = assemblyName + ".dll";

                    try
                    {
                        File.WriteAllBytes(assemblyPath, assemblyFileBytes);
                    }
                    catch (Exception) {
                        // we don't care if it can't be written. assume that means it is already there
                    }
                }
                else
                {
                    AssemblyResolver.Register(assemblyName, assemblyFileBytes);
                }

            }
        }

        public static void DumpAssemblies()
        {
            foreach (String assemblyKey in AssemblyResolver.RegisteredAssemblies)
            {
                AssemblyName assemblyFullName = new AssemblyName(assemblyKey);
                var assemblyPath = Path.Combine(AssemblyDirectory, assemblyFullName.Name + ".dll");
                if (!File.Exists(assemblyPath))
                {
                    bool isSuccess = false;
                    while (!isSuccess)
                    {
                        try
                        {
                            File.WriteAllBytes(assemblyPath, AssemblyResolver.GetAssemblyBytes(assemblyKey));
                            isSuccess = true;
                        }
                        catch (Exception)
                        {
                            isSuccess = false;
                        }
                    }
                }
            }
        }

        public static UserDLLsInfo BuildUserDLLs(string userLibraryPrefix)
        {
            UserDLLsInfo userDLLsInfo = new UserDLLsInfo();

            if (userLibraryPrefix == null || userLibraryPrefix.Equals("$"))
            {
                List<byte> userDLLsBuffer = new List<byte>();
                userDLLsBuffer.Add((byte)'$');
                userDLLsInfo.UserDLLsBuffer = Convert.ToBase64String(userDLLsBuffer.ToArray());
                userDLLsInfo.UserDLLsBufferInfo = "$";
            }
            else
            {
                List<byte> userDLLsBuffer = new List<byte>();
                StringBuilder userDLLsBufferInfo = new StringBuilder();

                var relatedAssemblies = GetRelatedApplicationAssemblies(userLibraryPrefix,
                                                                        GetExcludedAssemblies());
                foreach (var assembly in relatedAssemblies)
                {
                    byte[] assemblyNameBytes = Encoding.UTF8.GetBytes(assembly.Name);
                    userDLLsBuffer.AddRange(assemblyNameBytes);

                    FileStream fs = new FileStream(assembly.FilePath, FileMode.Open, FileAccess.Read);

                    using (BinaryReader br = new BinaryReader(fs))
                    {
                        byte[] assemblyFileBytes = br.ReadBytes(Convert.ToInt32(fs.Length));
                        userDLLsBuffer.AddRange(assemblyFileBytes);

                        userDLLsBufferInfo.Append(assembly.Name.Length);
                        userDLLsBufferInfo.Append('@');
                        userDLLsBufferInfo.Append(assemblyFileBytes.Length);
                        userDLLsBufferInfo.Append('@');
                    }
                }
                userDLLsInfo.UserDLLsBuffer = Convert.ToBase64String(userDLLsBuffer.ToArray());
                userDLLsInfo.UserDLLsBufferInfo = userDLLsBufferInfo.ToString();
            }
            return userDLLsInfo;
        }
    }
}
