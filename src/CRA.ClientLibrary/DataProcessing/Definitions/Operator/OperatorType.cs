namespace CRA.ClientLibrary.DataProcessing
{
    /// <summary>
    /// Operations that can be applied to an IShardedDataset
    /// </summary>
    public enum OperatorType : int
    {
        /// <summary>
        /// Transforms one IDataset to another IDataset type
        /// </summary>
        UnaryTransform,
        /// <summary>
        /// Transforms both IDataset1 and IDataset2 to another IDataset
        /// </summary>
        BinaryTransform,
        /// <summary>
        /// Moves data between two CRA sharded vertices
        /// </summary>
        Move,
        /// <summary>
        /// Splitting phase of the move operation between two CRA sharded vertices
        /// </summary>
        MoveSplit,
        /// <summary>
        /// Merging phase of the move operation between two CRA sharded vertices
        /// </summary>
        MoveMerge,
        /// <summary>
        /// Produces data to be used as an input by other sharded CRA vertices
        /// </summary>
        Produce,
        /// <summary>
        /// Subscribes an observer to the output data from other CRA sharded vertices to be used as an output
        /// </summary>
        Subscribe,
        /// <summary>
        /// Connects the client to the other CRA sharded vertices
        /// </summary>
        ClientTerminal,
        /// <summary>
        /// Peroforms no action on a sharded dataset
        /// </summary>
        None
    };
}
