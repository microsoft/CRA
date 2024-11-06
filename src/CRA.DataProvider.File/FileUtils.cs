namespace CRA.DataProvider.File
{
    using Newtonsoft.Json;
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Threading.Tasks;

    /// <summary>
    /// Definition for FileUtils
    /// </summary>
    public static class FileUtils
    {
        private static readonly JsonSerializer jsonSerializer
            = new JsonSerializer();

        public static string GetDirectory(string path)
        {
            if (!Directory.Exists(path))
            { Directory.CreateDirectory(path); }

            return path;
        }

        public static Stream GetReadWriteStream(string fileName)
        {
            if (string.IsNullOrWhiteSpace(fileName))
            {
                throw new ArgumentException("File name cannot be null or empty", nameof(fileName));
            }

            string directoryName = Path.GetDirectoryName(fileName);
            if (directoryName == null || !Directory.Exists(directoryName))
            {
                throw new DirectoryNotFoundException($"Directory does not exist: {directoryName}");
            }

            if (fileName.Contains("..") || fileName.Contains("/") || fileName.Contains("\\"))
            {
                throw new ArgumentException("Invalid file name", nameof(fileName));
            }

            return File.Open(fileName, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
        }

        public static Stream GetReadStream(string fileName)
        {
            if (string.IsNullOrWhiteSpace(fileName))
            {
                throw new ArgumentException("File name cannot be null or empty", nameof(fileName));
            }

            string directoryName = Path.GetDirectoryName(fileName);
            if (directoryName == null || !Directory.Exists(directoryName))
            {
                throw new DirectoryNotFoundException($"Directory does not exist: {directoryName}");
            }

            if (fileName.Contains("..") || fileName.Contains("/") || fileName.Contains("\\"))
            {
                throw new ArgumentException("Invalid file name", nameof(fileName));
            }

            return File.Open(fileName, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
        }

        public static void WriteList<T>(
            this Stream stream,
            IEnumerable<T> list)
        {
            stream.Position = 0;
            stream.SetLength(0);

            using (var writer = new StreamWriter(
                stream,
                System.Text.Encoding.UTF8,
                8000,
                true))
            {
                var jsonWriter = new JsonTextWriter(writer);
                jsonSerializer.Serialize(
                    jsonWriter,
                    list);
            }
        }

        public static List<T> ReadList<T>(this Stream stream)
        {
            if (stream.Length == 0)
            { return new List<T>(); }

            using (var reader = new StreamReader(
                stream,
                System.Text.Encoding.UTF8,
                true,
                8000,
                true))
            {
                var jsonReader = new JsonTextReader(reader);
                return jsonSerializer.Deserialize<List<T>>(jsonReader);
            }
        }

        public static string GetUpdateVersionId(string versionId)
        {
            if (versionId == null)
            { return "0"; }

            if (int.TryParse(versionId, out var vid))
            { return (vid + 1).ToString(); }

            return "0";
        }

        public static Task<int> CountAll<T>(
            string fileName)
        {
            using (var stream = FileUtils.GetReadStream(fileName))
            { return Task.FromResult(stream.ReadList<T>().Count); }
        }

        public static Task<T?> Get<T>(
            string fileName,
            Func<T, bool> match)
            where T : struct
        {
            var found = false;
            using (var stream = FileUtils.GetReadStream(fileName))
            {
                var list = stream.ReadList<T>();

                for (int idx = 0; idx < list.Count && !found; idx++)
                {
                    if (match(list[idx]))
                    { return Task.FromResult<T?>(list[idx]); }
                }
            }

            return Task.FromResult<T?>(null);
        }

        public static Task<List<T>> GetAll<T>(
            string fileName,
            Func<T, bool> match)
        {
            using (var stream = FileUtils.GetReadStream(fileName))
            {
                var list = stream.ReadList<T>();
                return Task.FromResult(list.Where(t => match(t)).ToList());
            }
        }


        public static Task<bool> Exists<T>(
            string fileName,
            T itemToFind,
            MatchForUpdate<T> matcher)
        {
            var found = false;
            using (var stream = FileUtils.GetReadStream(fileName))
            {
                var list = stream.ReadList<T>();

                for (int idx = 0; idx < list.Count && !found; idx++)
                {
                    var item = list[idx];
                    bool shouldUpdate = false;
                    (found, shouldUpdate) = matcher(item, itemToFind);
                }
            }

            return Task.FromResult(found);
        }

        public static Task InsertOrUpdate<T>(
            string _fileName,
            T itemToUpdate,
            MatchForUpdate<T> matcher,
            Func<T, T> cloneWithUpdateVersion)
        {
            using (var stream = FileUtils.GetReadWriteStream(_fileName))
            {
                var list = stream.ReadList<T>();

                var found = false;
                for (int idx = 0; idx < list.Count && !found; idx++)
                {
                    var item = list[idx];
                    bool shouldUpdate = false;
                    (found, shouldUpdate) = matcher(item, itemToUpdate);

                    if (shouldUpdate)
                    { list[idx] = cloneWithUpdateVersion(itemToUpdate); }
                }

                if (!found)
                { list.Add(cloneWithUpdateVersion(itemToUpdate)); }

                stream.WriteList(list);
            }

            return Task.FromResult(true);
        }

        public static Task DeleteItem<T>(
            string fileName,
            T itemToDelete,
            MatchForUpdate<T> matcher)
        {
            using (var stream = FileUtils.GetReadWriteStream(fileName))
            {
                var list = stream.ReadList<T>();

                var found = false;
                for (int idx = 0; idx < list.Count && !found; idx++)
                {
                    var item = list[idx];
                    bool shouldUpdate = false;
                    (found, shouldUpdate) = matcher(item, itemToDelete);

                    if (shouldUpdate)
                    { list.RemoveAt(idx); }
                }

                stream.WriteList(list);
            }

            return Task.FromResult(true);
        }
    }

    public delegate (bool matched, bool versionMatched) MatchForUpdate<T>(T dbItem, T otherItem);

}
