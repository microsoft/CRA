namespace CRA.ClientLibrary.DataProcessing
{
    public static class ParallelismUtils
    {
        public static int GetCoresCount()
        {
            int coresCount = 0;
#if false
            foreach (var item in new System.Management.ManagementObjectSearcher("Select * from Win32_Processor").Get())
                coresCount += int.Parse(item["NumberOfCores"].ToString());
#endif
            return coresCount;
        }
    }
}
