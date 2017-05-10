namespace CRA.ClientLibrary
{
    /// <summary>
    /// Error codes for CRA method calls
    /// </summary>
    public enum CRAErrorCode : int
    {
        /// <summary>
        /// Success
        /// </summary>
        Success,
        /// <summary>
        /// Process not found
        /// </summary>
        ProcessNotFound,
        /// <summary>
        /// Process output endpoint not found
        /// </summary>
        ProcessOutputNotFound,
        /// <summary>
        /// Process input endpoint not found
        /// </summary>
        ProcessInputNotFound,
        /// <summary>
        /// Process already exists
        /// </summary>
        ProcessAlreadyExists,
        /// <summary>
        /// Recovering
        /// </summary>
        ServerRecovering,
        /// <summary>
        /// Race condition adding connection
        /// </summary>
        ConnectionAdditionRace,
        /// <summary>
        /// Process endpoint (input or output) not found
        /// </summary>
        ProcessEndpointNotFound,
        /// <summary>
        /// Failed to establish a connection
        /// </summary>
        ConnectionEstablishFailed
    };
}
