using System;
using System.Collections.Concurrent;
using System.Linq.Expressions;
using CRA.ClientLibrary;
using CRA.ClientLibrary.AzureProvider;
using CRA.ClientLibrary.DataProcessing;

namespace ShardedDatasetTest
{
    class Program
    {
        static void Main(string[] args)
        {
            var storageConnectionString = Environment.GetEnvironmentVariable("AZURE_STORAGE_CONN_STRING");

            int workersCount = 2;
            DeploymentUtils.DefaultDeployDescriptor = new DeployDescriptorBase(CreateDefaultDeployer(workersCount));

            var shardedDatasetClient = new ShardedDatasetClient(new AzureProviderImpl(storageConnectionString));

            ShuffleTest1(shardedDatasetClient);
            
        }

        static private void TransformTest(ShardedDatasetClient client)
        {
            Expression<Func<int, IntKeyedDataset<int, int>>> sharder = x => new IntKeyedDataset<int, int>(x);
            var shardedIntKeyedDS = client.CreateShardedDataset<int, int, IntKeyedDataset<int, int>>(sharder);

            shardedIntKeyedDS.Transform<int, int, IntKeyedDataset<int, int>>(a => a.shiftUp(1))
                             .Deploy();
        }

        static private async void ShuffleTest1(ShardedDatasetClient client)
        {
            Expression<Func<int, IntKeyedDataset<int, int>>> sharder = x => new IntKeyedDataset<int, int>(x);
            var shardedIntKeyedDS = client.CreateShardedDataset<int, int, IntKeyedDataset<int, int>>(sharder);

            var deployedDS = await shardedIntKeyedDS.Transform<int, int, IntKeyedDataset<int, int>>(a => a.shiftUp(1))
                             .Move<int, int, IntKeyedDataset<int, int>, int, int, IntKeyedDataset<int, int>>(
                                    (s, d) => s.splitter(d), 
                                    (m, d) => m.merger(d),
                                    MoveDescriptor.MoveBase())
                             .Transform<int, int, IntKeyedDataset<int, int>>(a => a.shiftUp(1))
                             .Deploy();

            await deployedDS.Subscribe(() => new WriteToConsoleObserver<int, int>());
        }

        static private void ShuffleTest2(ShardedDatasetClient client)
        {
            Expression<Func<int, IntKeyedDataset<int, int>>> sharder = x => new IntKeyedDataset<int, int>(x);
            var shardedIntKeyedDS = client.CreateShardedDataset<int, int, IntKeyedDataset<int, int>>(sharder);

            shardedIntKeyedDS.Transform<int, int, IntKeyedDataset<int, int>>(a => a.shiftUp(1))
                             .Move<int, int, IntKeyedDataset<int, int>, int, int, IntKeyedDataset<int, int>>(
                                    (s, d) => s.splitter(d),
                                    (m, d) => m.merger(d),
                                    MoveDescriptor.MoveBase())
                             .Move<int, int, IntKeyedDataset<int, int>, int, int, IntKeyedDataset<int, int>>(
                                    (s, d) => s.splitter(d),
                                    (m, d) => m.merger(d),
                                    MoveDescriptor.MoveBase())
                             .Transform<int, int, IntKeyedDataset<int, int>>(a => a.shiftUp(1))
                             .Deploy();
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
