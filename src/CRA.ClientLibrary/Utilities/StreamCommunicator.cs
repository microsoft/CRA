using System;
using System.IO;
using System.Threading.Tasks;

namespace CRA.ClientLibrary
{
    /// <summary>
    /// Stream communication primitives
    /// </summary>
    public static class StreamCommunicator
    {
        /// <summary>
        /// Read integer fixed size
        /// </summary>
        /// <param name="stream"></param>
        /// <returns></returns>
        public static int ReadInt32Fixed(this Stream stream)
        {
            var value = new byte[4];
            stream.ReadAllRequiredBytes(value, 0, value.Length);
            int intValue = value[0]
                | (int)value[1] << 0x8
                | (int)value[2] << 0x10
                | (int)value[3] << 0x18;
            return intValue;
        }

        /// <summary>
        /// Write integer fixed size
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="value"></param>
        public static void WriteInt32Fixed(this Stream stream, int value)
        {
            stream.WriteByte((byte)(value & 0xFF));
            stream.WriteByte((byte)((value >> 0x8) & 0xFF));
            stream.WriteByte((byte)((value >> 0x10) & 0xFF));
            stream.WriteByte((byte)((value >> 0x18) & 0xFF));
        }

        /// <summary>
        /// Read integer compressed
        /// </summary>
        /// <param name="stream"></param>
        /// <returns></returns>
        public static int ReadInt32(this Stream stream)
        {
            var currentByte = (uint)stream.ReadByte();
            byte read = 1;
            uint result = currentByte & 0x7FU;
            int shift = 7;
            while ((currentByte & 0x80) != 0)
            {
                currentByte = (uint)stream.ReadByte();
                read++;
                result |= (currentByte & 0x7FU) << shift;
                shift += 7;
                if (read > 5)
                {
                    throw new InvalidOperationException("Invalid integer value in the input stream.");
                }
            }
            return (int)((-(result & 1)) ^ ((result >> 1) & 0x7FFFFFFFU));
        }

        /// <summary>
        /// Read integer compressed (Async)
        /// </summary>
        /// <param name="stream"></param>
        /// <returns></returns>
        public async static Task<int> ReadInt32Async(this Stream stream)
        {
            byte[] buffer = new byte[1];
            await stream.ReadAsync(buffer, 0, 1);
            var currentByte = (uint)buffer[0];
            byte read = 1;
            uint result = currentByte & 0x7FU;
            int shift = 7;
            while ((currentByte & 0x80) != 0)
            {
                byte[] tmpBuffer = new byte[1];
                await stream.ReadAsync(tmpBuffer, 0, 1);
                currentByte = (uint)tmpBuffer[0];
                read++;
                result |= (currentByte & 0x7FU) << shift;
                shift += 7;
                if (read > 5)
                {
                    throw new InvalidOperationException("Invalid integer value in the input stream.");
                }
            }
            return (int)((-(result & 1)) ^ ((result >> 1) & 0x7FFFFFFFU));
        }

        /// <summary>
        /// Write integer compressed
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="value"></param>
        public static void WriteInt32(this Stream stream, int value)
        {
            var zigZagEncoded = unchecked((uint)((value << 1) ^ (value >> 31)));
            while ((zigZagEncoded & ~0x7F) != 0)
            {
                stream.WriteByte((byte)((zigZagEncoded | 0x80) & 0xFF));
                zigZagEncoded >>= 7;
            }
            stream.WriteByte((byte)zigZagEncoded);
        }

        /// <summary>
        /// Write byte array
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="value"></param>
        public static void WriteByteArray(this Stream stream, byte[] value)
        {
            if (value == null)
            {
                throw new ArgumentNullException("value");
            }

            stream.WriteInt32(value.Length);
            if (value.Length > 0)
            {
                stream.Write(value, 0, value.Length);
            }
        }

        /// <summary>
        /// Read byte array
        /// </summary>
        /// <param name="stream"></param>
        /// <returns></returns>
        public static byte[] ReadByteArray(this Stream stream)
        {
            int arraySize = stream.ReadInt32();
            var array = new byte[arraySize];
            if (arraySize > 0)
            {
                stream.ReadAllRequiredBytes(array, 0, array.Length);
            }
            return array;
        }

        /// <summary>
        /// Read all required bytes
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="buffer"></param>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public static int ReadAllRequiredBytes(this Stream stream, byte[] buffer, int offset, int count)
        {
            int toRead = count;
            int currentOffset = offset;
            int currentRead;
            do
            {
                currentRead = stream.Read(buffer, currentOffset, toRead);
                currentOffset += currentRead;
                toRead -= currentRead;
            }
            while (toRead > 0 && currentRead != 0);
            return currentOffset - offset;
        }

        /// <summary>
        /// Read all required bytes (async version)
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="buffer"></param>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public static async Task<int> ReadAllRequiredBytesAsync(this Stream stream, byte[] buffer, int offset, int count)
        {
            int toRead = count;
            int currentOffset = offset;
            int currentRead;
            do
            {
                currentRead = await stream.ReadAsync(buffer, currentOffset, toRead);
                currentOffset += currentRead;
                toRead -= currentRead;
            }
            while (toRead > 0 && currentRead != 0);
            return currentOffset - offset;
        }
    }
}
