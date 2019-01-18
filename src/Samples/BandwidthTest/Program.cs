using System;
using CRA.ClientLibrary;

namespace BandwidthTest
{
    class Program
    {
        static void Main(string[] args)
        {
            var client = new CRAClientLibrary(
                new CRA.FileSyncDataProvider.FileProviderImpl());

            int chunkSize = 10*1024*1024;

            Console.ReadLine();
            client.DefineVertex("bandwidthtestvertex", () => new BandwidthTestVertex()).Wait();
            client.InstantiateVertex("crainst01", "bwvertex1", "bandwidthtestvertex", chunkSize).Wait();
            client.InstantiateVertex("crainst02", "bwvertex2", "bandwidthtestvertex", chunkSize).Wait();
            //client.InstantiateVertex("crainst03", "bwvertex3", "bandwidthtestvertex", chunkSize);

            client.Connect("bwvertex1", "output1",  "bwvertex2", "input1");
            client.Connect("bwvertex1", "output2",  "bwvertex2", "input2");
            client.Connect("bwvertex1", "output3",  "bwvertex2", "input3");
            client.Connect("bwvertex1", "output4",  "bwvertex2", "input4");
            client.Connect("bwvertex1", "output5",  "bwvertex2", "input5");
            client.Connect("bwvertex1", "output6",  "bwvertex2", "input6");
            client.Connect("bwvertex1", "output7",  "bwvertex2", "input7");
            client.Connect("bwvertex1", "output8",  "bwvertex2", "input8");
            client.Connect("bwvertex2", "output9",  "bwvertex1", "input9");
            client.Connect("bwvertex2", "output10", "bwvertex1", "input10");
            client.Connect("bwvertex2", "output11", "bwvertex1", "input11");
            client.Connect("bwvertex2", "output12", "bwvertex1", "input12");
            client.Connect("bwvertex2", "output13", "bwvertex1", "input13");
            client.Connect("bwvertex2", "output14", "bwvertex1", "input14");
            client.Connect("bwvertex2", "output15", "bwvertex1", "input15");
            client.Connect("bwvertex2", "output16", "bwvertex1", "input16");
            /**/

            Console.ReadLine();
            client.Reset();
        }
    }
}
