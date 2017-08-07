using System.Runtime.Serialization;

namespace CRA.ClientLibrary.DataProcessing
{
    [DataContract]
    public class ClientTerminalTask : TaskBase
    {
        public ClientTerminalTask() : base(OperatorType.ClientTerminal) { }
    }
}
