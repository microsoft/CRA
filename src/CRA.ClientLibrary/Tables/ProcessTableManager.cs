using System;
using System.Linq;
using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace CRA.ClientLibrary
{
    /// <summary>
    /// An assignment of one machine to a group
    /// </summary>
    public class ProcessTableManager
    {
        private CloudTable _processTable;

        internal ProcessTableManager(string storageConnectionString)
        {
            var _storageAccount = CloudStorageAccount.Parse(storageConnectionString);
            var _tableClient = _storageAccount.CreateCloudTableClient();
            _processTable = CreateTableIfNotExists("processtableforcra", _tableClient);
        }

        internal void DeleteTable()
        {
            _processTable.DeleteIfExists();
        }

        internal bool ExistsProcess(string processName)
        {
            TableQuery<ProcessTable> query = new TableQuery<ProcessTable>()
                .Where(TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, processName));
            return _processTable.ExecuteQuery(query).Any();
        }

        internal void RegisterInstance(string instanceName, string address, int port)
        {
            TableOperation insertOperation = TableOperation.InsertOrReplace(new ProcessTable
                (instanceName, "", "", address, port, "", "", true));
            _processTable.Execute(insertOperation);
        }

        internal void RegisterProcess(string processName, string instanceName)
        {
            TableOperation insertOperation = TableOperation.InsertOrReplace(new ProcessTable
                (instanceName, processName, "", "", 0, "", "", false));
            _processTable.Execute(insertOperation);
        }

        internal void ActivateProcessOnInstance(string processName, string instanceName)
        {
            var newActiveProcess = ProcessTable.GetAll(_processTable)
                .Where(gn => instanceName == gn.InstanceName && processName == gn.ProcessName)
                .First();

            newActiveProcess.IsActive = true;
            TableOperation insertOperation = TableOperation.InsertOrReplace(newActiveProcess);
            _processTable.Execute(insertOperation);

            var procs = ProcessTable.GetAll(_processTable)
                .Where(gn => processName == gn.ProcessName && instanceName != gn.InstanceName);
            foreach (var proc in procs)
            {
                if (proc.IsActive)
                {
                    proc.IsActive = false;
                    TableOperation _insertOperation = TableOperation.InsertOrReplace(proc);
                    _processTable.Execute(_insertOperation);
                }
            }
        }

        internal void DeactivateProcessOnInstance(string processName, string instanceName)
        {
            var newActiveProcess = ProcessTable.GetAll(_processTable)
                .Where(gn => instanceName == gn.InstanceName && processName == gn.ProcessName)
                .First();

            newActiveProcess.IsActive = false;
            TableOperation insertOperation = TableOperation.InsertOrReplace(newActiveProcess);
            _processTable.Execute(insertOperation);
        }

        internal void DeleteInstance(string instanceName)
        {
            var newRow = new DynamicTableEntity(instanceName, "");
            newRow.ETag = "*";
            TableOperation deleteOperation = TableOperation.Delete(newRow);
            _processTable.Execute(deleteOperation);
        }

        internal ProcessTable GetRowForActiveProcess(string processName)
        {
            return ProcessTable.GetAll(_processTable)
                .Where(gn => processName == gn.ProcessName && !string.IsNullOrEmpty(gn.InstanceName))
                .Where(gn => gn.IsActive)
                .First();
        }

        internal ProcessTable GetRowForInstance(string instanceName)
        {
            return GetRowForInstanceProcess(instanceName, "");
        }

        internal ProcessTable GetRowForInstanceProcess(string instanceName, string processName)
        {
            return ProcessTable.GetAll(_processTable).Where(gn => instanceName == gn.InstanceName && processName == gn.ProcessName).First();
        }

        internal ProcessTable GetRowForDefaultInstance()
        {
            return ProcessTable.GetAll(_processTable).Where(gn => string.IsNullOrEmpty(gn.ProcessName)).First();
        }

        private static CloudTable CreateTableIfNotExists(string tableName, CloudTableClient _tableClient)
        {
            CloudTable table = _tableClient.GetTableReference(tableName);
            try
            {
                table.CreateIfNotExists();
            }
            catch (Exception)
            {
            }

            return table;
        }

        internal List<string> GetProcessNames()
        {
            TableQuery<ProcessTable> query = new TableQuery<ProcessTable>()
                .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.NotEqual, ""));
            return _processTable.ExecuteQuery(query).Select(e => e.ProcessName).ToList();
        }

        internal List<string> GetProcessDefinitions()
        {
            TableQuery<ProcessTable> query = new TableQuery<ProcessTable>()
                .Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, ""));
            return _processTable.ExecuteQuery(query).Select(e => e.ProcessName).ToList();
        }

        internal List<string> GetInstanceNames()
        {
            TableQuery<ProcessTable> query = new TableQuery<ProcessTable>()
                .Where(TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, ""));
            return _processTable.ExecuteQuery(query).Select(e => e.InstanceName).ToList();
        }
    }
}
