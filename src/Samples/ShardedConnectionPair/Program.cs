using System;
using CRA.ClientLibrary;

namespace ShardedConnectionPair
{
    class Program
    {
        static void Main(string[] args)
        {
            var client = new CRAClientLibrary();

            client.DefineVertexAsync("shardedconnectionpairvertex", () => new ShardedConnectionPairVertex()).Wait();

            client.InstantiateVertexAsync(
                new[] { "crainst01", "crainst02" },
                "vertex1", 
                "shardedconnectionpairvertex",
                null, 1, e => e % 2).Wait();

            client.InstantiateVertexAsync(
                new[] { "crainst01", "crainst02" }, 
                "vertex2", 
                "shardedconnectionpairvertex", 
                null, 1, e => e % 2).Wait();

            client.ConnectAsync("vertex1", "output", "vertex2", "input").Wait();
            client.ConnectAsync("vertex2", "output", "vertex1", "input").Wait();

            Console.ReadLine();
        }
    }
}
