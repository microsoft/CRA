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

            /*
            client.Connect("bwprocess1", "output2", "bwprocess2", "input2");
            client.Connect("bwprocess1", "output3", "bwprocess2", "input3");
            client.Connect("bwprocess1", "output4", "bwprocess2", "input4");
            client.Connect("bwprocess1", "output5", "bwprocess2", "input5");
            client.Connect("bwprocess1", "output6", "bwprocess2", "input6");
            client.Connect("bwprocess1", "output7", "bwprocess2", "input7");
            client.Connect("bwprocess1", "output8", "bwprocess2", "input8");
            client.Connect("bwprocess1", "output9", "bwprocess2", "input9");
            client.Connect("bwprocess1", "output10", "bwprocess2", "input10");
            client.Connect("bwprocess1", "output11", "bwprocess2", "input11");
            client.Connect("bwprocess1", "output12", "bwprocess2", "input12");
            
            client.Connect("bwprocess1", "output13", "bwprocess2", "input13");
            client.Connect("bwprocess1", "output14", "bwprocess2", "input14");
            client.Connect("bwprocess1", "output15", "bwprocess2", "input15");
            client.Connect("bwprocess1", "output16", "bwprocess2", "input16");*/

            Console.ReadLine();
        }
    }
}
