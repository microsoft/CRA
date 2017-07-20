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

            client.DefineProcess("fusableconnectionpairprocess", () => new FusableConnectionPairProcess());

            client.InstantiateProcess("crainst01", "fprocess1", "fusableconnectionpairprocess", null);
            client.InstantiateProcess("crainst01", "fprocess2", "fusableconnectionpairprocess", null);

            client.Connect("fprocess1", "output", "fprocess2", "input");

            Console.ReadLine();
        }
    }
}
