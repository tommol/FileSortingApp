// See https://aka.ms/new-console-template for more information

using System.Diagnostics;
using TomaszMolis.FileSort.Generator;

Stopwatch stopwatch = new Stopwatch();
stopwatch.Start();
if (args.Length == 0)
{
    Console.WriteLine("Please provide desired length of file in bytes.");
    return;
}
var validLength = int.TryParse(args[0], out int length);
if (!validLength)
{
    Console.WriteLine("Desired length must be an integer.");
}

if (length <= 0)
{
    Console.WriteLine("Length must be positive integer.");
}
Console.WriteLine($"Generating file with lenght of {length}B");
var fileCreator = new FileCreator();

var output = await fileCreator.Create(length);
stopwatch.Stop();
Console.WriteLine($"Output file: {output}");
Console.WriteLine($"Total time: {stopwatch.Elapsed}");