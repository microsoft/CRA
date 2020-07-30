using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net.NetworkInformation;
using System.Net;
using System.Threading.Tasks;
using System.Net.Sockets;

namespace CRA.ClientLibrary
{
    public static class NetworkUtils
    {
        internal static async Task ConnectAsync(this TcpClient tcpClient, string host, int port, int timeoutMs)
        {
            var cancelTask = Task.Delay(timeoutMs);
            var connectTask = tcpClient.ConnectAsync(host, port);
            await await Task.WhenAny(connectTask, cancelTask);
            if (cancelTask.IsCompleted)
            {
                throw new Exception("Timed out");
            }
        }

        /* Copyright 2018 mniak (https://gist.github.com/jrusbatch/4211535)*/
        public static int GetAvailablePort(int startingPort = 5000)
        {
            var properties = IPGlobalProperties.GetIPGlobalProperties();

            //getting active connections
            var tcpConnectionPorts = properties.GetActiveTcpConnections()
                                .Where(n => n.LocalEndPoint.Port >= startingPort)
                                .Select(n => n.LocalEndPoint.Port);

            //getting active tcp listners - WCF service listening in tcp
            var tcpListenerPorts = properties.GetActiveTcpListeners()
                                .Where(n => n.Port >= startingPort)
                                .Select(n => n.Port);

            //getting active udp listeners
            var udpListenerPorts = properties.GetActiveUdpListeners()
                                .Where(n => n.Port >= startingPort)
                                .Select(n => n.Port);

            var port = Enumerable.Range(startingPort, ushort.MaxValue)
                .Where(i => !tcpConnectionPorts.Contains(i))
                .Where(i => !tcpListenerPorts.Contains(i))
                .Where(i => !udpListenerPorts.Contains(i))
                .FirstOrDefault();

            return port;
        }
    }
}
