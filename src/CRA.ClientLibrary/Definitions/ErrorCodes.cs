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
        /// Vertex not found
        /// </summary>
        VertexNotFound,
        /// <summary>
        /// ActiveVertex not found
        /// </summary>
        ActiveVertexNotFound,
        /// <summary>
        /// Vertex output endpoint not found
        /// </summary>
        VertexOutputNotFound,
        /// <summary>
        /// Vertex input endpoint not found
        /// </summary>
        VertexInputNotFound,
        /// <summary>
        /// Vertex already exists
        /// </summary>
        VertexAlreadyExists,
        /// <summary>
        /// Recovering
        /// </summary>
        ServerRecovering,
        /// <summary>
        /// Race condition adding connection
        /// </summary>
        ConnectionAdditionRace,
        /// <summary>
        /// Vertex endpoint (input or output) not found
        /// </summary>
        VertexEndpointNotFound,
        /// <summary>
        /// Failed to establish a connection
        /// </summary>
        ConnectionEstablishFailed,
        /// <summary>
        /// Failed to match endpoints between fromVertices and toVertices
        /// </summary>
        VerticesEndpointsNotMatched
    };
}
