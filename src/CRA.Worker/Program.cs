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
            //Console.WriteLine("Press ENTER to start");
            //Console.ReadLine();

            if (args.Length != 3)
            {
                Console.WriteLine("Worker for Common Runtime for Applications (CRA)\nUsage: CRA.Worker.exe instancename (e.g., instance1) ipaddress (e.g., 127.0.0.1) port (e.g., 11000)");
                return;
            }

            if (args[1] == "null")
            {
                args[1] = GetLocalIPAddress();
            }
            Debug.WriteLine("Worker instance name is: " + args[0]);
            Debug.WriteLine("Using IP address: " + args[1] + " and port " + Convert.ToInt32(args[2]));
            Debug.WriteLine("Using Azure connection string: " + ConfigurationManager.AppSettings.Get("StorageConnectionString"));

            var worker = new CRAWorker
                (args[0], args[1], Convert.ToInt32(args[2]),
                ConfigurationManager.AppSettings.Get("StorageConnectionString"));


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
