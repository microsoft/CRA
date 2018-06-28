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

            client.DefineVertex("connectionpairvertex", () => new ConnectionPairVertex());

            client.InstantiateVertex("crainst01", "vertex1", "connectionpairvertex", new MyParam { field1 = 33, field2 = "foo" });


            client.InstantiateVertex("crainst02", "vertex2", "connectionpairvertex", new MyParam { field1 = 34 });

            client.Connect("vertex1", "output", "vertex2", "input");
            client.Connect("vertex2", "output", "vertex1", "input");

            Console.ReadLine();
        }
    }
}
