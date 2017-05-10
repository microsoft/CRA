namespace CRA.ClientLibrary
{
    /// <summary>
    /// Direction of data flow
    /// </summary>
    public enum ConnectionInitiator
    {
        /// <summary>
        /// Initiate connection from "from" process
        /// </summary>
        FromSide,

        /// <summary>
        /// Initiate connection from "to" process
        /// </summary>
        ToSide
    }
}
