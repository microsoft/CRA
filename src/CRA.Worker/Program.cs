using System;
using System.Net;
using System.Net.Sockets;
using System.Configuration;
using System.Diagnostics;
using CRA.ClientLibrary;
using System.Reflection;

namespace CRA.Worker
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length < 2)
            {
                Console.WriteLine("Worker for Common Runtime for Applications (CRA) [http://github.com/Microsoft/CRA]");
                Console.WriteLine("Usage: CRA.Worker.exe instancename (e.g., instance1) port (e.g., 11000) [ipaddress (null for default)] [secure_network_assembly_name secure_network_class_name]");
                return;
            }

            string ipAddress = "";

            if (args.Length < 3 || args[2] == "null")
            {
                ipAddress = GetLocalIPAddress();
            }
            else
            {
                ipAddress = args[2];
            }


            string storageConnectionString = ConfigurationManager.AppSettings.Get("AZURE_STORAGE_CONN_STRING");

            if (storageConnectionString == null)
            {
                storageConnectionString = Environment.GetEnvironmentVariable("AZURE_STORAGE_CONN_STRING");
            }

            if (storageConnectionString == null)
            {
                throw new InvalidOperationException("Azure storage connection string not found. Use appSettings in your app.config to provide this using the key AZURE_STORAGE_CONN_STRING, or use the environment variable AZURE_STORAGE_CONN_STRING.");
            }

            int connectionsPoolPerWorker;
            string connectionsPoolPerWorkerString = ConfigurationManager.AppSettings.Get("CRA_WORKER_MAX_CONN_POOL");
            if (connectionsPoolPerWorkerString != null)
            {
                try
                {
                    connectionsPoolPerWorker = Convert.ToInt32(connectionsPoolPerWorkerString);
                }
                catch
                {
                    throw new InvalidOperationException("Maximum number of connections per CRA worker is wrong. Use appSettings in your app.config to provide this using the key CRA_WORKER_MAX_CONN_POOL.");
                }
            }
            else
            {
                connectionsPoolPerWorker = 1000;
            }

            ISecureStreamConnectionDescriptor descriptor = null;
            if (args.Length > 3)
            {
                if (args.Length < 5)
                    throw new InvalidOperationException("Invalid secure network info provided");

                descriptor = (ISecureStreamConnectionDescriptor)Activator.CreateInstance(args[4], args[5]).Unwrap();
            }

            Console.WriteLine("Starting CRA Worker [http://github.com/Microsoft/CRA]");
            Console.WriteLine("   Worker Instance Name: " + args[0]);
            Console.WriteLine("   IP address: " + ipAddress);
            Console.WriteLine("   Port: " + Convert.ToInt32(args[1]));
            Console.WriteLine("   Azure connection string: " + storageConnectionString);

            if (descriptor != null)
                Console.WriteLine("   Secure network connections enabled using assembly=" + args[4] + "; type=" + args[5]);
            else
                if (args.Length > 3)
                    Console.WriteLine("   WARNING: Secure network could not be configured");

            var worker = new CRAWorker
                (args[0], ipAddress, Convert.ToInt32(args[1]),
                storageConnectionString, descriptor, connectionsPoolPerWorker);

            worker.Start();
        }

        private static string GetLocalIPAddress()
        {
            var host = Dns.GetHostEntry(Dns.GetHostName());
            foreach (var ip in host.AddressList)
            {
                if (ip.AddressFamily == AddressFamily.InterNetwork)
                {
                    return ip.ToString();
                }
            }
            throw new InvalidOperationException("Local IP Address Not Found!");
        }
    }
}
