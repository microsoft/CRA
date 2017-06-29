using System;
using CRA.ClientLibrary;

namespace BandwidthTest
{
    class Program
    {
        static void Main(string[] args)
        {
            var client = new CRAClientLibrary();
            int chunkSize = 10*1024*1024;
            client.Reset();

            client.DefineProcess("bandwidthtestprocess", () => new BandwidthTestProcess());

            client.InstantiateProcess("crainst01", "bwprocess1", "bandwidthtestprocess", chunkSize);
            client.InstantiateProcess("crainst02", "bwprocess2", "bandwidthtestprocess", chunkSize);
            //client.InstantiateProcess("crainst03", "bwprocess3", "bandwidthtestprocess", chunkSize);

            client.Connect("bwprocess1", "output1", "bwprocess2", "input1");
            //client.Connect("bwprocess1", "output2", "bwprocess2", "input2");
            //client.Connect("bwprocess1", "output3", "bwprocess2", "input3");
            // client.Connect("bwprocess3", "output1", "bwprocess2", "input2");

            Console.ReadLine();
        }
    }
}
