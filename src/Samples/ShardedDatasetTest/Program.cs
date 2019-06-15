using System;
using System.Collections.Concurrent;
using System.Linq.Expressions;
using CRA.ClientLibrary;
using CRA.DataProvider.Azure;
using CRA.ClientLibrary.DataProcessing;

namespace ShardedDatasetTest
{
    class Program
    {
        static void Main(string[] args)
        {
            var storageConnectionString = Environment.GetEnvironmentVariable("AZURE_STORAGE_CONN_STRING");

            // We assume two CRA instances with names: crainst0, crainst1
            int workersCount = 2;
            DeploymentUtils.DefaultDeployDescriptor = new DeployDescriptorBase(CreateDefaultDeployer(workersCount));

            var shardedDatasetClient = new ShardedDatasetClient(new AzureDataProvider(storageConnectionString));

            ProduceTest(shardedDatasetClient);
        }

        static private async void ProduceTest(ShardedDatasetClient client)
        {
            Expression<Func<int, IntKeyedDataset<int, int>>> sharder = x => new IntKeyedDataset<int, int>(x);
            var shardedIntKeyedDS = client.CreateShardedDataset<int, int, IntKeyedDataset<int, int>>(sharder);

            var deployedDS = await shardedIntKeyedDS.Deploy();

            await deployedDS.Subscribe(() => new WriteToConsoleObserver<int, int>());
        }


        static private async void TransformTest1(ShardedDatasetClient client)
        {
            Expression<Func<int, IntKeyedDataset<int, int>>> sharder = x => new IntKeyedDataset<int, int>(x);
            var shardedIntKeyedDS = client.CreateShardedDataset<int, int, IntKeyedDataset<int, int>>(sharder);

            var deployedDS = await shardedIntKeyedDS.Transform<int, int, IntKeyedDataset<int, int>>(a => a.ShiftUp(1))
                             .Deploy();

            await deployedDS.Subscribe(() => new WriteToConsoleObserver<int, int>());
        }

        static private async void TransformTest2(ShardedDatasetClient client)
        {
            Expression<Func<int, IntKeyedDataset<int, int>>> sharder = x => new IntKeyedDataset<int, int>(x);
            var shardedIntKeyedDS = client.CreateShardedDataset<int, int, IntKeyedDataset<int, int>>(sharder);
            var shardedIntKeyedDS1 = client.CreateShardedDataset<int, int, IntKeyedDataset<int, int>>(sharder)
                                            .Transform<int, int, IntKeyedDataset<int, int>>(a => a.ShiftUp(1));
            var shardedIntKeyedDS2 = client.CreateShardedDataset<int, int, IntKeyedDataset<int, int>>(sharder)
                                            .Transform<int, int, IntKeyedDataset<int, int>>(a => a.ShiftUp(1));

            var deployedDS = await shardedIntKeyedDS.Transform<int, int, IntKeyedDataset<int, int>>(a => a.ShiftUp(1))
                             .Transform<int, int, IntKeyedDataset<int, int>, int, int, IntKeyedDataset<int, int>>(shardedIntKeyedDS1, (a, b) => a.BinaryShiftUp(b, 1))
                             .Move<int, int, IntKeyedDataset<int, int>, int, int, IntKeyedDataset<int, int>>(
                                    (s, d) => s.Splitter(d),
                                    (m, d) => m.Merger(d),
                                    MoveDescriptor.MoveBase())
                             .Transform<int, int, IntKeyedDataset<int, int>>(a => a.ShiftUp(1))
                             .Transform<int, int, IntKeyedDataset<int, int>, int, int, IntKeyedDataset<int, int>>(shardedIntKeyedDS2, (a, b) => a.BinaryShiftUp(b, 1))
                             .Deploy();

            await deployedDS.Subscribe(() => new WriteToConsoleObserver<int, int>());
        }

        static private async void ShuffleTest1(ShardedDatasetClient client)
        {
            Expression<Func<int, IntKeyedDataset<int, int>>> sharder = x => new IntKeyedDataset<int, int>(x);
            var shardedIntKeyedDS = client.CreateShardedDataset<int, int, IntKeyedDataset<int, int>>(sharder);

            var deployedDS = await shardedIntKeyedDS.Transform<int, int, IntKeyedDataset<int, int>>(a => a.ShiftUp(1))
                             .Move<int, int, IntKeyedDataset<int, int>, int, int, IntKeyedDataset<int, int>>(
                                    (s, d) => s.Splitter(d), 
                                    (m, d) => m.Merger(d),
                                    MoveDescriptor.MoveBase())
                             .Transform<int, int, IntKeyedDataset<int, int>>(a => a.ShiftUp(1))
                             .Deploy();

            await deployedDS.Subscribe(() => new WriteToConsoleObserver<int, int>());
        }

        static private async void ShuffleTest2(ShardedDatasetClient client)
        {
            Expression<Func<int, IntKeyedDataset<int, int>>> sharder = x => new IntKeyedDataset<int, int>(x);
            var shardedIntKeyedDS = client.CreateShardedDataset<int, int, IntKeyedDataset<int, int>>(sharder);

            var deployedDS = await shardedIntKeyedDS.Transform<int, int, IntKeyedDataset<int, int>>(a => a.ShiftUp(1))
                             .Move<int, int, IntKeyedDataset<int, int>, int, int, IntKeyedDataset<int, int>>(
                                    (s, d) => s.Splitter(d),
                                    (m, d) => m.Merger(d),
                                    MoveDescriptor.MoveBase())
                             .Move<int, int, IntKeyedDataset<int, int>, int, int, IntKeyedDataset<int, int>>(
                                    (s, d) => s.Splitter(d),
                                    (m, d) => m.Merger(d),
                                    MoveDescriptor.MoveBase())
                             .Transform<int, int, IntKeyedDataset<int, int>>(a => a.ShiftUp(1))
                             .Deploy();

            await deployedDS.Subscribe(() => new WriteToConsoleObserver<int, int>());
        }

        private static string[] CreateInstancesNames(int craWorkersCount)
        {
            string[] instancesNames = new string[craWorkersCount];
            for (int i = 0; i < craWorkersCount; i++)
                instancesNames[i] = ConfigurationsManager.CRA_WORKER_PREFIX + i;
            return instancesNames;
        }

        private static ConcurrentDictionary<string, int> CreateDefaultDeployer(int workersCount)
        {
            ConcurrentDictionary<string, int> deployShards = new ConcurrentDictionary<string, int>();
            for (int i = 0; i < workersCount; i++)
                deployShards.AddOrUpdate(ConfigurationsManager.CRA_WORKER_PREFIX + i, ConfigurationsManager.INSTANCE_COUNT_PER_CRA_WORKER,
                                (inst, proc) => ConfigurationsManager.INSTANCE_COUNT_PER_CRA_WORKER);
            return deployShards;
        }
    }
}
