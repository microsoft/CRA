using System;
using CRA.ClientLibrary;

namespace ShardedConnectionPair
{
    class Program
    {
        static void Main(string[] args)
        {
            var client = new CRAClientLibrary();

            client.DefineVertex("shardedconnectionpairvertex", () => new ShardedConnectionPairVertex());

            client.InstantiateVertex(
                new[] { "crainst01", "crainst02" },
                "vertex1", 
                "shardedconnectionpairvertex",
                null, 1, e => e % 2);
            client.InstantiateVertex(
                new[] { "crainst01", "crainst02" }, 
                "vertex2", 
                "shardedconnectionpairvertex", 
                null, 1, e => e % 2);

            client.Connect("vertex1", "output", "vertex2", "input");
            client.Connect("vertex2", "output", "vertex1", "input");

            Console.ReadLine();
        }
    }
}
