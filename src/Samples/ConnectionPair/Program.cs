using System;
using CRA.ClientLibrary;

namespace ConnectionPair
{
    class Program
    {
        static void Main(string[] args)
        {
            var client = new CRAClientLibrary();

            client.DefineProcess("myfirstprocess", () => new MyFirstProcess());
            client.DefineProcess("mysecondprocess", () => new MySecondProcess());

            client.InstantiateProcess("instance1", "myprocess1", "myfirstprocess", null);
            client.InstantiateProcess("instance2", "myprocess2", "mysecondprocess", null);

            client.Connect("myprocess1", "firstoutput", "myprocess2", "secondinput");
            client.Connect("myprocess2", "secondoutput", "myprocess1", "firstinput");

            Console.ReadLine();
        }
    }
}
