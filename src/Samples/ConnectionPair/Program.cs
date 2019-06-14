using System;
using CRA.ClientLibrary;

namespace ConnectionPair
{
    public struct MyParam
    {
        public int field1;
        public string field2;
    }

    class Program
    {
        static void Main(string[] args)
        {
            var client = new CRAClientLibrary();

            client.DefineVertexAsync("connectionpairvertex", () => new ConnectionPairVertex()).Wait();

            client.InstantiateVertexAsync("crainst01", "vertex1", "connectionpairvertex", new MyParam { field1 = 33, field2 = "foo" }).Wait();


            client.InstantiateVertexAsync("crainst02", "vertex2", "connectionpairvertex", new MyParam { field1 = 34 }).Wait();

            client.ConnectAsync("vertex1", "output", "vertex2", "input").Wait();
            client.ConnectAsync("vertex2", "output", "vertex1", "input").Wait();

            Console.ReadLine();
        }
    }
}
