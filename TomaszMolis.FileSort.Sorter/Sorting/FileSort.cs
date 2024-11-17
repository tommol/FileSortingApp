using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TomaszMolis.FileSort.Sorter.Data;

namespace TomaszMolis.FileSort.Sorter.Sorting
{
    public class FileSort : ISorter
    {
        private int CHUNK_SIZE = 8000;

        public async Task<TimeSpan> SortAsync(string inputFilePath, string outputFilePath)
        {

            FileInfo inputFile = new FileInfo(inputFilePath);

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            try
            {

                CHUNK_SIZE = (int)Math.Log(inputFile.Length)* CHUNK_SIZE;        
                Stopwatch splitting = new Stopwatch();
                splitting.Start();
                List<string> tempFiles = await SplitAlphabeticaly(inputFilePath);
                splitting.Stop();
                Console.WriteLine("Splitted into {0} files", tempFiles.Count);
                Console.WriteLine($"Splitting took {splitting.Elapsed}.");
                Stopwatch sorting = new Stopwatch();
                sorting.Start();
                var sortedFiles = SortFiles(tempFiles);
                sorting.Stop();
                Console.WriteLine($"Sorting took {sorting.Elapsed}.");
                Stopwatch merging = new Stopwatch();
                merging.Start();
                MergeFiles(sortedFiles, outputFilePath);
                merging.Stop();
                Console.WriteLine($"Merging took {merging.Elapsed}.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
            }
            finally
            {
                stopwatch.Stop();

            }
            return stopwatch.Elapsed;
        }

        List<string> Split(string inputFile)
        {
            List<string> tempFiles = new List<string>();
            List<string> currentChunk = new List<string>();
            List<Task> tasks = new List<Task>();

            using (StreamReader reader = new StreamReader(inputFile))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    currentChunk.Add(line);

                    // If we reach the chunk size, sort it and write it to a temporary file
                    if (currentChunk.Count >= CHUNK_SIZE)
                    {
                        string tempFile = Path.GetTempFileName();
                        tempFiles.Add(tempFile);

                        WriteSplitted(currentChunk, tempFile);

                        currentChunk.Clear();
                    }
                }

                // If there are any remaining lines in the last chunk, sort and write them
                if (currentChunk.Count > 0)
                {
                    string tempFile = Path.GetTempFileName();
                    tempFiles.Add(tempFile);
                    var sortTask = new Task(() =>
                    {
                        WriteSplitted(currentChunk, tempFile);
                    });
                    tasks.Add(sortTask);
                }
            }

            return tempFiles;
        }

        private async Task<List<string>> SplitAlphabeticaly(string inputFile)
        {
            string outputDirectory = "OutputFiles";
            Directory.CreateDirectory(outputDirectory);
            var fileContents = new ConcurrentDictionary<char, ConcurrentBag<string>>();
            for (char letter = 'A'; letter <= 'Z'; letter++)
            {
                fileContents[letter] = new ConcurrentBag<string>();
            }
            
            const int bufferSize = 8120; // Buffer size in bytes
            using (var stream = new FileStream(inputFile, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize, useAsync: true))
            using (var reader = new StreamReader(stream))
            {
                while (!reader.EndOfStream)
                {
                    // Read a chunk of lines asynchronously
                    var chunk = await ReadLinesChunkAsync(reader, 2000); // Adjust chunk size as needed
                    Parallel.ForEach(chunk, line =>
                    {
                        if (line == null || string.IsNullOrWhiteSpace(line.Text))
                            return;

                        char firstChar = char.ToUpper(line.Text[0]);
                        if (firstChar >= 'A' && firstChar <= 'Z')
                        {
                            fileContents[firstChar].Add(line.ToString());
                        }
                    });
                }
            }
            Parallel.ForEach(fileContents.Keys, letter =>
            {
                string outputPath = Path.Combine(outputDirectory, $"{letter}.txt");
                File.WriteAllLines(outputPath, fileContents[letter]);
            });

            var files = fileContents.Keys.Select(k => $"{outputDirectory}/{k}.txt").ToList();
            var nonempty = files.Where(f => new FileInfo(f).Length > 0).ToList();
            foreach (var toDelete in files.Where(f => new FileInfo(f).Length ==0))
            {
                File.Delete(toDelete);
            }
            
            return nonempty;
        }
        
        private static async Task<LineItem[]> ReadLinesChunkAsync(StreamReader reader, int chunkSize)
        {
            var lines = new LineItem[chunkSize];
            int count = 0;

            for (int i = 0; i < chunkSize && !reader.EndOfStream; i++)
            {
                lines[count++] = LineItem.Parse(await reader.ReadLineAsync());
            }

            return lines.Take(count).ToArray();
        }
        List<(char Letter, string Name)> SortFiles(List<string> tempFiles)
        {
            List<(char,string)> sortedFiles = new List<(char,string)>();
            List<Task> tasks = new List<Task>();
            foreach (var file in tempFiles)
            {
                var sortTask = new Task(() =>
                {
                    var lines = File.ReadAllLines(file).Select(LineItem.Parse).ToList();
                    var sortedLines = SortItems(lines);
                    string tempFile = Path.GetTempFileName();
                    sortedFiles.Add((char.ToUpper(lines[0].Text[0]), tempFile));
                    File.WriteAllLines(tempFile, sortedLines.Select(l => l.ToString()));
                });
                tasks.Add(sortTask);
                sortTask.Start();
            }
            Task.WaitAll(tasks.ToArray());
            foreach (var file in tempFiles)
            {
                File.Delete(file);
            }
            return sortedFiles;
        }

        static void WriteSplitted(List<string> chunk, string tempFile)
        {
            using (StreamWriter writer = new StreamWriter(tempFile))
            {
                foreach (var item in chunk)
                {
                    writer.WriteLine(item);
                }
            }
        }

        private static List<LineItem> SortItems(List<LineItem> items)
        {
            LineItem[] input = items.ToArray();
            int n = items.Count;
            LineItem[] temp = new LineItem[n];
            for (int size = 1; size < n; size *= 2)
            {
                for (int leftStart = 0; leftStart < n - size; leftStart += 2 * size)
                {
                    int mid = Math.Min(leftStart + size - 1, n - 1);
                    int rightEnd = Math.Min(leftStart + 2 * size - 1, n - 1);

                    Merge(input, temp, leftStart, mid, rightEnd);
                }
            }
            return input.ToList();
        }

        static void Merge(LineItem[] array, LineItem[] temp, int leftStart, int mid, int rightEnd)
        {
            int left = leftStart;
            int right = mid + 1;
            int index = leftStart;

            // Merge the two subarrays into temp[]
            while (left <= mid && right <= rightEnd)
            {
                int comparison = string.CompareOrdinal(array[left].Text, array[right].Text);
                if (comparison < 0)
                {
                    temp[index] = array[left];
                    left++;
                }
                else if (comparison == 0)
                {
                    if (array[left].Number <= array[right].Number)
                    {
                        temp[index] = array[left];
                        left++;
                    }
                    else
                    {
                        temp[index] = array[right];
                        right++;
                    }
                }
                else
                {
                    temp[index] = array[right];
                    right++;
                }
                index++;
            }

            // Copy any remaining elements from the left subarray
            while (left <= mid)
            {
                temp[index] = array[left];
                left++;
                index++;
            }

            // Copy any remaining elements from the right subarray
            while (right <= rightEnd)
            {
                temp[index] = array[right];
                right++;
                index++;
            }

            // Copy the merged subarray back into the original array
            for (int i = leftStart; i <= rightEnd; i++)
            {
                array[i] = temp[i];
            }
        }

        static void MergeFiles(List<(char Letter, string Name)> tempFiles, string outputFile)
        {
            var ordered = tempFiles.OrderBy(f=>f.Letter).Select(x=>x.Name).ToList();
            using (var outputStream = new FileStream(outputFile, FileMode.Create, FileAccess.Write))
            {
                foreach (string filePath in ordered)
                {
                    // Open the input file and copy its contents to the output file
                    using (var inputStream = new FileStream(filePath, FileMode.Open, FileAccess.Read))
                    {
                        inputStream.CopyTo(outputStream);
                    }
                }
            }

        }
    }
}
