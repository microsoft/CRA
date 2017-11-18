using System;
using CRA.ClientLibrary;

namespace FusableConnectionPair
{
    class Program
    {
        static void Main(string[] args)
        {
            var client = new CRAClientLibrary();
            client.Reset();

            client.DefineVertex("fusableconnectionpairvertex", () => new FusableConnectionPairVertex());

            client.InstantiateVertex("crainst01", "fvertex1", "fusableconnectionpairvertex", null);
            client.InstantiateVertex("crainst01", "fvertex2", "fusableconnectionpairvertex", null);

            client.Connect("fvertex1", "output", "fvertex2", "input");

            Console.ReadLine();
        }
    }
}
