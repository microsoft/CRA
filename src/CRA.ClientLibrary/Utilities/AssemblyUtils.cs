using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Collections.Generic;
using System.Diagnostics.Contracts;
using System.Globalization;
using System.Runtime.Serialization;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;
using System.Threading;
using System.Collections.Concurrent;

namespace CRA.ClientLibrary
{
    public static class TableExtensions
    {
        public static IList<T> ExecuteQuery<T>(this CloudTable table, TableQuery<T> query) where T : ITableEntity, new()
        {
            return table.ExecuteQueryAsync(query).GetAwaiter().GetResult();
        }

        public static async Task<IList<T>> ExecuteQueryAsync<T>(this CloudTable table, TableQuery<T> query, CancellationToken ct = default(CancellationToken), Action<IList<T>> onProgress = null) where T : ITableEntity, new()
        {

            var items = new List<T>();
            TableContinuationToken token = null;

            do
            {
                TableQuerySegment<T> seg = table.ExecuteQuerySegmentedAsync<T>(query, token).GetAwaiter().GetResult();
                //TableQuerySegment<T> seg = await table.ExecuteQuerySegmentedAsync<T>(query, token);

                token = seg.ContinuationToken;
                items.AddRange(seg);
                if (onProgress != null) onProgress(items);

            } while (token != null && !ct.IsCancellationRequested);

            return items;
        }
    }

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

            string[] excludedCoreAssemblies = new[]
            {
                "System.Private.CoreLib, Version=4.0.0.0, Culture=neutral, PublicKeyToken=7cec85d7bea7798e",
                "api-ms-win-core-console-l1-1-0",
                "api-ms-win-core-datetime-l1-1-0",
                "api-ms-win-core-debug-l1-1-0",
                "api-ms-win-core-errorhandling-l1-1-0",
                "api-ms-win-core-file-l1-1-0",
                "api-ms-win-core-file-l1-2-0",
                "api-ms-win-core-file-l2-1-0",
                "api-ms-win-core-handle-l1-1-0",
                "api-ms-win-core-heap-l1-1-0",
                "api-ms-win-core-interlocked-l1-1-0",
                "api-ms-win-core-libraryloader-l1-1-0",
                "api-ms-win-core-localization-l1-2-0",
                "api-ms-win-core-memory-l1-1-0",
                "api-ms-win-core-namedpipe-l1-1-0",
                "api-ms-win-core-processenvironment-l1-1-0",
                "api-ms-win-core-processthreads-l1-1-0",
                "api-ms-win-core-processthreads-l1-1-1",
                "api-ms-win-core-profile-l1-1-0",
                "api-ms-win-core-rtlsupport-l1-1-0",
                "api-ms-win-core-string-l1-1-0",
                "api-ms-win-core-synch-l1-1-0",
                "api-ms-win-core-synch-l1-2-0",
                "api-ms-win-core-sysinfo-l1-1-0",
                "api-ms-win-core-timezone-l1-1-0",
                "api-ms-win-core-util-l1-1-0",
                "api-ms-win-crt-conio-l1-1-0",
                "api-ms-win-crt-convert-l1-1-0",
                "api-ms-win-crt-environment-l1-1-0",
                "api-ms-win-crt-filesystem-l1-1-0",
                "api-ms-win-crt-heap-l1-1-0",
                "api-ms-win-crt-locale-l1-1-0",
                "api-ms-win-crt-math-l1-1-0",
                "api-ms-win-crt-multibyte-l1-1-0",
                "api-ms-win-crt-private-l1-1-0",
                "api-ms-win-crt-process-l1-1-0",
                "api-ms-win-crt-runtime-l1-1-0",
                "api-ms-win-crt-stdio-l1-1-0",
                "api-ms-win-crt-string-l1-1-0",
                "api-ms-win-crt-time-l1-1-0",
                "api-ms-win-crt-utility-l1-1-0",
                "clrcompression",
                "clretwrc",
                "clrjit",
                "coreclr",
                "dbgshim",
                "hostpolicy",
                "Microsoft.DiaSymReader.Native.amd64",
                "Microsoft.Win32.Primitives",
                "Microsoft.Win32.Registry",
                "mscordaccore",
                "mscordaccore_amd64_amd64_4.6.26328.01",
                "mscordbi",
                "mscorrc.debug",
                "netstandard",
                "sos",
                "SOS.NETCore",
                "sos_amd64_amd64_4.6.26328.01",
                "System.AppContext",
                "System.Buffers",
                "System.Collections.Concurrent",
                "System.Collections.Immutable",
                "System.Collections.NonGeneric",
                "System.Collections.Specialized",
                "System.ComponentModel.Annotations",
                "System.ComponentModel.Composition",
                "System.ComponentModel.EventBasedAsync",
                "System.ComponentModel.Primitives",
                "System.ComponentModel.TypeConverter",
                "System.Console",
                "System.Data.Common",
                "System.Diagnostics.Contracts",
                "System.Diagnostics.Debug",
                "System.Diagnostics.DiagnosticSource",
                "System.Diagnostics.FileVersionInfo",
                "System.Diagnostics.Process",
                "System.Diagnostics.StackTrace",
                "System.Diagnostics.TextWriterTraceListener",
                "System.Diagnostics.Tools",
                "System.Diagnostics.TraceSource",
                "System.Diagnostics.Tracing",
                "System.Drawing.Primitives",
                "System.Dynamic.Runtime",
                "System.Globalization.Calendars",
                "System.Globalization.Extensions",
                "System.IO.Compression",
                "System.IO.Compression.FileSystem",
                "System.IO.Compression.ZipFile",
                "System.IO.FileSystem.AccessControl",
                "System.IO.FileSystem.DriveInfo",
                "System.IO.FileSystem.Primitives",
                "System.IO.FileSystem.Watcher",
                "System.IO.IsolatedStorage",
                "System.IO.MemoryMappedFiles",
                "System.IO.Pipes",
                "System.IO.UnmanagedMemoryStream",
                "System.Linq",
                "System.Linq.Expressions",
                "System.Linq.Parallel",
                "System.Linq.Queryable",
                "System.Net.HttpListener",
                "System.Net.Mail",
                "System.Net.NameResolution",
                "System.Net.NetworkInformation",
                "System.Net.Ping",
                "System.Net.Primitives",
                "System.Net.Requests",
                "System.Net.Security",
                "System.Net.ServicePoint",
                "System.Net.Sockets",
                "System.Net.WebClient",
                "System.Net.WebHeaderCollection",
                "System.Net.WebProxy",
                "System.Net.WebSockets.Client",
                "System.Numerics.Vectors",
                "System.ObjectModel",
                "System.Private.DataContractSerialization",
                "System.Private.Uri",
                "System.Private.Xml",
                "System.Private.Xml.Linq",
                "System.Reflection.DispatchProxy",
                "System.Reflection.Emit",
                "System.Reflection.Emit.ILGeneration",
                "System.Reflection.Emit.Lightweight",
                "System.Reflection.Extensions",
                "System.Reflection.Metadata",
                "System.Reflection.Primitives",
                "System.Reflection.TypeExtensions",
                "System.Resources.Reader",
                "System.Resources.ResourceManager",
                "System.Resources.Writer",
                "System.Runtime.CompilerServices.VisualC",
                "System.Runtime.Extensions",
                "System.Runtime.Handles",
                "System.Runtime.InteropServices",
                "System.Runtime.InteropServices.RuntimeInformation",
                "System.Runtime.InteropServices.WindowsRuntime",
                "System.Runtime.Loader",
                "System.Runtime.Numerics",
                "System.Runtime.Serialization.Json",
                "System.Runtime.Serialization.Primitives",
                "System.Runtime.Serialization.Xml",
                "System.Security.AccessControl",
                "System.Security.Claims",
                "System.Security.Cryptography.Algorithms",
                "System.Security.Cryptography.Cng",
                "System.Security.Cryptography.Csp",
                "System.Security.Cryptography.Encoding",
                "System.Security.Cryptography.OpenSsl",
                "System.Security.Cryptography.Primitives",
                "System.Security.Cryptography.X509Certificates",
                "System.Security.Principal",
                "System.Security.Principal.Windows",
                "System.Security.SecureString",
                "System.ServiceModel.Web",
                "System.Text.Encoding",
                "System.Text.Encoding.Extensions",
                "System.Text.RegularExpressions",
                "System.Threading",
                "System.Threading.Overlapped",
                "System.Threading.Tasks.Dataflow",
                "System.Threading.Tasks.Extensions",
                "System.Threading.Tasks.Parallel",
                "System.Threading.Thread",
                "System.Threading.ThreadPool",
                "System.Threading.Timer",
                "System.Transactions.Local",
                "System.ValueTuple",
                "System.Web.HttpUtility",
                "System.Xml.ReaderWriter",
                "System.Xml.Serialization",
                "System.Xml.XDocument",
                "System.Xml.XmlDocument",
                "System.Xml.XmlSerializer",
                "System.Xml.XPath",
                "System.Xml.XPath.XDocument",
                "ucrtbase",
                "WindowsBase",
                "System.Private.Uri, Version=4.0.4.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Runtime, Version=4.2.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Runtime.InteropServices, Version=4.2.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Security.Principal, Version=4.1.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Runtime.Extensions, Version=4.2.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Diagnostics.Debug, Version=4.1.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Threading.Tasks, Version=4.1.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Resources.ResourceManager, Version=4.1.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Threading, Version=4.1.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Text.Encoding.Extensions, Version=4.1.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Console, Version=4.1.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Collections, Version=4.1.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Reflection.Emit.ILGeneration, Version=4.0.3.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Diagnostics.Tools, Version=4.1.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Reflection.Emit, Version=4.1.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.ObjectModel, Version=4.1.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Reflection.Primitives, Version=4.1.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Reflection.Emit.Lightweight, Version=4.0.3.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Linq, Version=4.2.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Linq.Expressions, Version=4.2.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Threading.Thread, Version=4.1.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Runtime, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Threading.Overlapped, Version=4.1.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Buffers, Version=4.0.2.0, Culture=neutral, PublicKeyToken=cc7b13ffcd2ddd51",
                "System.IO.FileSystem, Version=4.1.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.IO.MemoryMappedFiles, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.IO.Pipes, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Threading.ThreadPool, Version=4.1.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Collections.NonGeneric, Version=4.1.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.ComponentModel, Version=4.0.3.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.ComponentModel.Primitives, Version=4.2.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Diagnostics.FileVersionInfo, Version=4.0.2.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Collections.Specialized, Version=4.1.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Security.Claims, Version=4.1.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "Microsoft.Win32.Primitives, Version=4.1.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Security.Principal.Windows, Version=4.1.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Security.AccessControl, Version=4.1.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "Microsoft.Win32.Registry, Version=4.1.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Security.Cryptography.Primitives, Version=4.1.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Diagnostics.Tracing, Version=4.2.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Collections.Concurrent, Version=4.0.14.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Security.Cryptography.Encoding, Version=4.1.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Security.Cryptography.Algorithms, Version=4.3.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Security.Cryptography.Cng, Version=4.3.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Runtime.Numerics, Version=4.1.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Net.Primitives, Version=4.1.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Security.Cryptography.Csp, Version=4.1.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Security.Cryptography.X509Certificates, Version=4.2.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Net.Security, Version=4.1.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Drawing.Primitives, Version=4.1.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Threading.Timer, Version=4.1.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Diagnostics.TraceSource, Version=4.1.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Runtime.Serialization.Formatters, Version=4.0.2.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Resources.Writer, Version=4.1.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Text.RegularExpressions, Version=4.2.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.ComponentModel.TypeConverter, Version=4.2.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Diagnostics.Process, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Security.Cryptography.X509Certificates, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Runtime.Extensions, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Diagnostics.Tools, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Collections, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Collections.NonGeneric, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Collections.Concurrent, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.ObjectModel, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Collections.Specialized, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.ComponentModel.TypeConverter, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.ComponentModel.EventBasedAsync, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.ComponentModel.Primitives, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.ComponentModel, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "Microsoft.Win32.Primitives, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Console, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Net.WebHeaderCollection, Version=4.1.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Diagnostics.DiagnosticSource, Version=4.0.2.1, Culture=neutral, PublicKeyToken=cc7b13ffcd2ddd51",
                "System.IO.Compression, Version=4.2.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089",
                "System.Net.Http, Version=4.2.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Net.ServicePoint, Version=4.0.0.0, Culture=neutral, PublicKeyToken=cc7b13ffcd2ddd51",
                "System.Net.NameResolution, Version=4.1.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Net.Sockets, Version=4.2.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Net.Requests, Version=4.1.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Threading.Tasks.Extensions, Version=4.1.1.0, Culture=neutral, PublicKeyToken=cc7b13ffcd2ddd51",
                "System.Private.Xml, Version=4.0.0.0, Culture=neutral, PublicKeyToken=cc7b13ffcd2ddd51",
                "System.Xml.ReaderWriter, Version=4.2.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Xml.XmlSerializer, Version=4.1.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Transactions.Local, Version=4.0.0.0, Culture=neutral, PublicKeyToken=cc7b13ffcd2ddd51",
                "System.Data.Common, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Runtime.InteropServices, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Diagnostics.TraceSource, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Diagnostics.Contracts, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Diagnostics.Debug, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Diagnostics.TextWriterTraceListener, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Diagnostics.FileVersionInfo, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Collections.Immutable, Version=1.2.2.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.IO.MemoryMappedFiles, Version=4.1.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Reflection.Metadata, Version=1.4.2.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Diagnostics.StackTrace, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Diagnostics.Tracing, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Drawing.Primitives, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Linq.Expressions, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.IO.Compression, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089",
                "System.IO.Compression.ZipFile, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089",
                "System.IO.FileSystem, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.IO.FileSystem.DriveInfo, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.IO.FileSystem.Watcher, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.IO.FileSystem.AccessControl, Version=4.0.2.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.IO.IsolatedStorage, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Linq, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Linq.Queryable, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Linq.Parallel, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Threading.Thread, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Net.Requests, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Net.Primitives, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Net.WebSockets, Version=4.1.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Net.HttpListener, Version=0.0.0.0, Culture=neutral, PublicKeyToken=cc7b13ffcd2ddd51",
                "System.Net.ServicePoint, Version=0.0.0.0, Culture=neutral, PublicKeyToken=cc7b13ffcd2ddd51",
                "System.Net.NameResolution, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.ComponentModel.EventBasedAsync, Version=4.1.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Net.WebClient, Version=0.0.0.0, Culture=neutral, PublicKeyToken=cc7b13ffcd2ddd51",
                "System.Net.Http, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Net.WebHeaderCollection, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Net.NetworkInformation, Version=4.2.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Net.WebProxy, Version=0.0.0.0, Culture=neutral, PublicKeyToken=cc7b13ffcd2ddd51",
                "System.Net.Mail, Version=0.0.0.0, Culture=neutral, PublicKeyToken=cc7b13ffcd2ddd51",
                "System.Net.NetworkInformation, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Net.Ping, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Net.Security, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Net.Sockets, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Net.WebSockets.Client, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Net.WebSockets, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Runtime.Numerics, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Threading.Tasks, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Reflection.Primitives, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Resources.ResourceManager, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Resources.Writer, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Runtime.CompilerServices.VisualC, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Runtime.InteropServices.RuntimeInformation, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Runtime.Serialization.Primitives, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Runtime.Serialization.Primitives, Version=4.2.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Private.Xml.Linq, Version=4.0.0.0, Culture=neutral, PublicKeyToken=cc7b13ffcd2ddd51",
                "System.Xml.XDocument, Version=4.1.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Private.DataContractSerialization, Version=4.1.3.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Runtime.Serialization.Xml, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Runtime.Serialization.Json, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Runtime.Serialization.Formatters, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Security.Claims, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Security.Cryptography.Algorithms, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Security.Cryptography.Csp, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Security.Cryptography.Encoding, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Security.Cryptography.Primitives, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Security.Principal, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Text.Encoding.Extensions, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Text.RegularExpressions, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Threading, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Threading.Overlapped, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Threading.ThreadPool, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Threading.Tasks.Parallel, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Threading.Timer, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Transactions.Local, Version=0.0.0.0, Culture=neutral, PublicKeyToken=cc7b13ffcd2ddd51",
                "System.Web.HttpUtility, Version=0.0.0.0, Culture=neutral, PublicKeyToken=cc7b13ffcd2ddd51",
                "System.Xml.ReaderWriter, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Xml.XDocument, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Xml.XmlSerializer, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Xml.XPath.XDocument, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Xml.XPath, Version=0.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "netstandard, Version=2.0.0.0, Culture=neutral, PublicKeyToken=cc7b13ffcd2ddd51",
                "System.Globalization, Version=4.1.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.IO, Version=4.2.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Data.Common, Version=4.2.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Text.Encoding, Version=4.1.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "Microsoft.IntelliTrace.TelemetryObserver.Common, Version=15.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "Microsoft.IntelliTrace.Concord",
                "Microsoft.IntelliTrace.ConfigUI",
                "Microsoft.IntelliTrace.Debugger.Common",
                "Microsoft.IntelliTrace.DebuggerMargin",
                "Microsoft.IntelliTrace.Package.Common",
                "Microsoft.IntelliTrace.Profiler",
                "Microsoft.IntelliTrace.SearchMargin",
                "Microsoft.IntelliTrace.TelemetryObserver.CoreClr",
                "Microsoft.VisualStudio.DefaultDataQueries",
                "Microsoft.VisualStudio.Diagnostics.TfsSymbolResolver",
                "Microsoft.VisualStudio.Diagnostics.Utilities",
                "Microsoft.VisualStudio.VIL",
                "Microsoft.VisualStudio.VIL.NotifyPointInProcHost",
                "System.Runtime, Version=4.0.20.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Resources.ResourceManager, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Reflection, Version=4.0.10.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Diagnostics.Debug, Version=4.0.10.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Collections, Version=4.0.10.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Runtime.Extensions, Version=4.0.10.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Reflection.Extensions, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Linq, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Threading, Version=4.0.10.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "Microsoft.IntelliTrace.TelemetryObserver.CoreClr, Version=15.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Reflection, Version=4.2.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Reflection.Extensions, Version=4.1.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.Diagnostics.StackTrace, Version=4.1.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "System.IO.FileSystem.Primitives, Version=4.1.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
            };


            string[] excludedAssemblies = new[]
            {
                "mscorlib, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089",
                "System, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089",
                "System.Core, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b77a5c561934e089",
                "Microsoft.Azure.KeyVault.Core, Version=1.0.0.0, Culture=neutral, PublicKeyToken=31bf3856ad364e35",
                "Microsoft.VisualStudio.HostingProcess.Utilities, Version=14.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
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
                "Microsoft.VisualStudio.HostingProcess.Utilities.Sync, Version=14.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
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
                "System.ServiceProcess, Version=4.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
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
                "Microsoft.WindowsAzure.Storage, Version=9.2.0.0, Culture=neutral, PublicKeyToken=31bf3856ad364e35",
                "Microsoft.WindowsAzure.Storage, Version=9.3.2.0, Culture=neutral, PublicKeyToken=31bf3856ad364e35",
                "Microsoft.Data.Services.Client, Version=5.8.1.0, Culture=neutral, PublicKeyToken=31bf3856ad364e35",
                "Microsoft.Data.Edm, Version=5.8.1.0, Culture=neutral, PublicKeyToken=31bf3856ad364e35",
                "Microsoft.Data.OData, Version=5.8.1.0, Culture=neutral, PublicKeyToken=31bf3856ad364e35",
                "System.Spatial, Version=5.8.1.0, Culture=neutral, PublicKeyToken=31bf3856ad364e35",
                "Newtonsoft.Json, Version=6.0.0.0, Culture=neutral, PublicKeyToken=30ad4fe6b2a6aeed",
                "Newtonsoft.Json, Version=8.0.0.0, Culture=neutral, PublicKeyToken=30ad4fe6b2a6aeed",
                "Newtonsoft.Json, Version=10.0.0.0, Culture=neutral, PublicKeyToken=30ad4fe6b2a6aeed",
                "Newtonsoft.Json, Version=11.0.0.0, Culture=neutral, PublicKeyToken=30ad4fe6b2a6aeed",
                "Newtonsoft.Json, Version=12.0.0.0, Culture=neutral, PublicKeyToken=30ad4fe6b2a6aeed",

                "Remote.Linq, Version=5.1.0.0, Culture=neutral, PublicKeyToken=null",
                "Aqua, Version=2.0.0.0, Culture=neutral, PublicKeyToken=null",
                "Anonymously Hosted DynamicMethods Assembly, Version=0.0.0.0, Culture=neutral, PublicKeyToken=null",
                "Remote.Linq, Version=5.3.1.0, Culture=neutral, PublicKeyToken=null",
                "Aqua, Version=3.0.0.0, Culture=neutral, PublicKeyToken=null",
                "Aqua.TypeSystem.Emit.Types, Version=0.0.0.0, Culture=neutral, PublicKeyToken=null",
                "Remote.Linq, Version=5.4.0.0, Culture=neutral, PublicKeyToken=null",
                "Aqua, Version=4.0.0.0, Culture=neutral, PublicKeyToken=null",
                "Aqua, Version=4.0.0.0, Culture=neutral, PublicKeyToken=82d6c51b67e8b655",
                "Remote.Linq, Version=5.0.0.0, Culture=neutral, PublicKeyToken=82d6c51b67e8b655",

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
                "CRA.ClientLibrary, Version=1.0.0.0, Culture=neutral, PublicKeyToken=cc877ef3899064b2",
                "Microsoft.VisualStudio.Debugger.Runtime, Version=15.0.0.0, Culture=neutral, PublicKeyToken=b03f5f7f11d50a3a",
                "Microsoft.VisualStudio.Debugger.Runtime.Impl",
            };

#if DOTNETCORE
            return excludedCoreAssemblies.Concat(excludedAssemblies).ToArray();
#else
            return excludedAssemblies;
#endif
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
            else
            {
                // Print included assembly name to console
                // Console.WriteLine(name);
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
            var relatedAssemblies = GetRelatedApplicationAssemblies(
                "",
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

        static ConcurrentDictionary<string, bool> assemblyLock = new ConcurrentDictionary<string, bool>();

        public static void LoadAssembliesFromStream(Stream stream)
        {
            int numAssemblies = stream.ReadInt32();

            string allAssemblies = "";
            for (int i = 0; i < numAssemblies; i++)
            {
                byte[] assemblyNameBytes = stream.ReadByteArray();
                string assemblyName = Encoding.UTF8.GetString(assemblyNameBytes);

                allAssemblies += "\"" + assemblyName + "\",\n";
                
                var managedAssembly = stream.ReadByte() == 0 ? false : true;

                byte[] assemblyFileBytes = stream.ReadByteArray();


                AssemblyName assemblyFullName = new AssemblyName(assemblyName);
                var assemblyPath = Path.Combine(AssemblyDirectory, assemblyFullName.Name + ".dll");

                if (assemblyName.Equals(assemblyFullName.Name))
                {   //this means the public key info etc is missing
                    //this assembly is probably an uploaded project reference
                    //just skip over it, the actual assembly will show up on the stream
                    continue;
                }

                if (assemblyLock.TryAdd(assemblyPath, true))
                {
                    try
                    {
                        File.WriteAllBytes(assemblyPath, assemblyFileBytes);
                    }
                    catch (Exception)
                    {
                        Console.WriteLine("INFO: Unable to update " + assemblyFullName.Name + ".dll (perhaps another CRA worker holds the file lock?)");
                    }

                    if (managedAssembly)
                    {
                        AssemblyResolver.Register(assemblyName, assemblyFileBytes);
                        Assembly.Load(assemblyName);
                    }
                }
            }
        }

        public static void DumpAssemblies()
        {
            foreach (String assemblyKey in AssemblyResolver.RegisteredAssemblies)
            {
                AssemblyName assemblyFullName = new AssemblyName(assemblyKey);
                var assemblyPath = Path.Combine(AssemblyDirectory, assemblyFullName.Name + ".dll");
                try
                {
                    Console.WriteLine("INFO: Updated " + assemblyFullName.Name + ".dll");

                    File.WriteAllBytes(assemblyPath, AssemblyResolver.GetAssemblyBytes(assemblyKey));
                }
                catch (Exception e)
                {
                    Console.WriteLine("INFO: Unable to update " + assemblyFullName.Name + ".dll -- " + e.ToString());
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
