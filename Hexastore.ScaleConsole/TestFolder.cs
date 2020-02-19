using System;
using System.IO;

namespace Hexastore.TestCommon
{
    /// <summary>
    /// Temp directory that cleans up on dispose.
    /// </summary>
    public class TestFolder : IDisposable
    {
        // The actual root
        private readonly DirectoryInfo _parent;

        public string Root => RootDirectory.FullName;

        /// <summary>
        /// Delete the directory after completion.
        /// </summary>
        public bool CleanUp { get; set; } = true;

        /// <summary>
        /// Root directory, parent of the working directory
        /// </summary>
        public DirectoryInfo RootDirectory { get; }

        public TestFolder()
        {
            var parts = Guid.NewGuid().ToString().ToLowerInvariant().Split('-');

            _parent = new DirectoryInfo(Path.Combine(Path.GetTempPath(), parts[0]));
            _parent.Create();

            File.WriteAllText(Path.Combine(_parent.FullName, "trace.txt"), Environment.StackTrace);

            RootDirectory = new DirectoryInfo(Path.Combine(_parent.FullName, parts[1]));
            RootDirectory.Create();
        }

        public static implicit operator string(TestFolder folder)
        {
            return folder.Root;
        }

        public override string ToString()
        {
            return Root;
        }

        public void Dispose()
        {
            if (CleanUp)
            {
                try
                {
                    _parent.Delete(true);
                }
                catch
                {
                }
            }
        }
    }
}