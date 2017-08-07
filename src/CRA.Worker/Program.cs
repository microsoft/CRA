using System;
using System.Net;
using System.Net.Sockets;
using System.Configuration;
using System.Diagnostics;
using CRA.ClientLibrary;

namespace CRA.Worker
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length < 2)
            {
                Console.WriteLine("Worker for Common Runtime for Applications (CRA)\nUsage: CRA.Worker.exe instancename (e.g., instance1) port (e.g., 11000) [ipaddress (e.g., 127.0.0.1)]");
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

            Debug.WriteLine("Worker instance name is: " + args[0]);
            Debug.WriteLine("Using IP address: " + ipAddress + " and port " + Convert.ToInt32(args[1]));

            // Load CRA worker settings 
            string storageConnectionString = ConfigurationManager.AppSettings.Get("CRA_STORAGE_CONN_STRING");
            if (storageConnectionString == null)
            {
                storageConnectionString = Environment.GetEnvironmentVariable("CRA_STORAGE_CONN_STRING");
            }

            if (storageConnectionString == null)
            {
                throw new InvalidOperationException("CRA storage connection string not found. Use appSettings in your app.config to provide this using the key CRA_STORAGE_CONN_STRING, or use the environment variable CRA_STORAGE_CONN_STRING.");
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

            Debug.WriteLine("Using Azure connection string: " + storageConnectionString);

            var worker = new CRAWorker
                (args[0], ipAddress, Convert.ToInt32(args[1]),
                storageConnectionString, connectionsPoolPerWorker);


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
