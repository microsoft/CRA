namespace CRA.ClientLibrary
{
    internal enum CRATaskMessageType
    {
        LOAD_VERTEX,
        CONNECT_VERTEX_INITIATOR,
        CONNECT_VERTEX_RECEIVER,
        CONNECT_VERTEX_INITIATOR_REVERSE,
        CONNECT_VERTEX_RECEIVER_REVERSE,
        PING,
        READY,
        RELEASE
    };

}
