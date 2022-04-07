// See https://aka.ms/new-console-template for more information

namespace RocksExplorer
{
    using System;
    using CommandLine;

    public class Program
    {
        public static void Main(string[] args)
        {
            Parser.Default.ParseArguments<ReadOptions, WriteOptions>(args).MapResult(
                (ReadOptions readOptions) => {
                    if (readOptions.File == null) {
                        Console.WriteLine("Provide database folder name");
                        return 1;
                    }

                    if (readOptions.Outfile == null) {
                        Console.WriteLine("Provide output file name");
                        return 1;
                    }

                    var provider = new RocksDbProvider(readOptions.File);
                    provider.Read(readOptions.Skip ?? 0, readOptions.Take ?? 1000, readOptions.Outfile).Wait();
                    return 0;
                },
                (WriteOptions writeOptions) => {
                    if (writeOptions.File == null) {
                        Console.WriteLine("Provide filename");
                        return 1;
                    }

                    var provider = new RocksDbProvider(writeOptions.File);
                    provider.Write(writeOptions.Count ?? 1000);
                    return 0;
                },
                e => 1);
        }
    }

    [Verb("read")]
    public class ReadOptions
    {
        [Value(0)]
        public string? File { get; set; }

        [Option('s', "skip")]
        public int? Skip { get; set; }

        [Option('t', "take")]
        public int? Take { get; set; }

        [Option('o', "outfile")]
        public string? Outfile { get; set; }
    }

    [Verb("write")]
    public class WriteOptions
    {
        [Value(0)]
        public string? File { get; set; }

        [Option('c', "count")]
        public int? Count { get; set; }
    }
}
