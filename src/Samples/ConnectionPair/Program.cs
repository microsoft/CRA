using System;
using CRA.ClientLibrary;

namespace ConnectionPair
{
    class Program
    {
        static void Main(string[] args)
        {
            var client = new CRAClientLibrary();

            client.DefineVertex("connectionpairvertex", () => new ConnectionPairVertex());

            client.InstantiateVertex("crainst01", "vertex1", "connectionpairvertex", null);
            client.InstantiateVertex("crainst02", "vertex2", "connectionpairvertex", null);

            client.Connect("vertex1", "output", "vertex2", "input");
            client.Connect("vertex2", "output", "vertex1", "input");

            Console.ReadLine();
        }
    }
}
