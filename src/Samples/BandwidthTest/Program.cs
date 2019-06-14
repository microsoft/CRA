using System;
using CRA.ClientLibrary;

namespace BandwidthTest
{
    class Program
    {
        static void Main(string[] args)
        {
            var client = new CRAClientLibrary(new CRA.FileSyncDataProvider.FileProviderImpl());

            int chunkSize = 10*1024*1024;

            Console.ReadLine();

            client.DefineVertexAsync("bandwidthtestvertex", () => new BandwidthTestVertex()).Wait();
            client.InstantiateVertexAsync("crainst01", "bwvertex1", "bandwidthtestvertex", chunkSize).Wait();
            client.InstantiateVertexAsync("crainst02", "bwvertex2", "bandwidthtestvertex", chunkSize).Wait();
            // client.InstantiateVertexAsync("crainst03", "bwvertex3", "bandwidthtestvertex", chunkSize).Wait();

            client.ConnectAsync("bwvertex1", "output1",  "bwvertex2", "input1").Wait();
            client.ConnectAsync("bwvertex1", "output2",  "bwvertex2", "input2").Wait();
            client.ConnectAsync("bwvertex1", "output3",  "bwvertex2", "input3").Wait();
            client.ConnectAsync("bwvertex1", "output4",  "bwvertex2", "input4").Wait();
            client.ConnectAsync("bwvertex1", "output5",  "bwvertex2", "input5").Wait();
            client.ConnectAsync("bwvertex1", "output6",  "bwvertex2", "input6").Wait();
            client.ConnectAsync("bwvertex1", "output7",  "bwvertex2", "input7").Wait();
            client.ConnectAsync("bwvertex1", "output8",  "bwvertex2", "input8").Wait();
            client.ConnectAsync("bwvertex2", "output9",  "bwvertex1", "input9").Wait();
            client.ConnectAsync("bwvertex2", "output10", "bwvertex1", "input10").Wait();
            client.ConnectAsync("bwvertex2", "output11", "bwvertex1", "input11").Wait();
            client.ConnectAsync("bwvertex2", "output12", "bwvertex1", "input12").Wait();
            client.ConnectAsync("bwvertex2", "output13", "bwvertex1", "input13").Wait();
            client.ConnectAsync("bwvertex2", "output14", "bwvertex1", "input14").Wait();
            client.ConnectAsync("bwvertex2", "output15", "bwvertex1", "input15").Wait();
            client.ConnectAsync("bwvertex2", "output16", "bwvertex1", "input16").Wait();

            Console.ReadLine();
            client.ResetAsync().Wait();
        }
    }
}
