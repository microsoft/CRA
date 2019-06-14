using System;
using CRA.ClientLibrary;

namespace FusableConnectionPair
{
    class Program
    {
        static void Main(string[] args)
        {
            var client = new CRAClientLibrary();
            client.ResetAsync().Wait();

            client.DefineVertexAsync("fusableconnectionpairvertex", () => new FusableConnectionPairVertex()).Wait();

            client.InstantiateVertexAsync("crainst01", "fvertex1", "fusableconnectionpairvertex", null).Wait();
            client.InstantiateVertexAsync("crainst01", "fvertex2", "fusableconnectionpairvertex", null).Wait();

            client.ConnectAsync("fvertex1", "output", "fvertex2", "input").Wait();

            Console.ReadLine();
        }
    }
}
