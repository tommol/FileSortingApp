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
        string outputDirectory = "OutputFiles";
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
                var sortedFiles =await  SortFiles(tempFiles);
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
            Directory.CreateDirectory(outputDirectory);
            var fileContents = new ConcurrentDictionary<char, StreamWriter>();
            for (char letter = 'A'; letter <= 'Z'; letter++)
            {
                var fileName = $"{outputDirectory}/{letter}.txt";
                fileContents[letter] =new StreamWriter(new FileStream(fileName, FileMode.Create, FileAccess.Write));
            }
            
            const int bufferSize = 8120; // Buffer size in bytes
            await using (var stream = new FileStream(inputFile, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize, useAsync: true))
            using (var reader = new StreamReader(stream))
            {
                while (!reader.EndOfStream)
                {
                    // Read a chunk of lines asynchronously
                    var chunk = await ReadLinesChunkAsync(reader, 2000); // Adjust chunk size as needed
                    Parallel.ForEach(chunk, new ParallelOptions(){ MaxDegreeOfParallelism = Environment.ProcessorCount},line =>
                    {
                        if (line == null || string.IsNullOrWhiteSpace(line.Text))
                            return;

                        char firstChar = char.ToUpper(line.Text[0]);
                        if (firstChar >= 'A' && firstChar <= 'Z')
                        {
                            lock (fileContents[firstChar])
                            {
                                fileContents[firstChar].WriteLine(line.ToString());
                            }
                        }
                    });
                }
            }

            foreach (var file in fileContents.Values)
            {
                file.Close();
            }

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

        async Task<List<(char Letter, string Name)>> SortFiles(List<string> tempFiles)
        {
            List<(char Letter, string Name)> result = new List<(char Letter, string Name)>();
            List<(char,string)> sortedFiles = new List<(char,string)>();
            var ordered = tempFiles.OrderBy(f=>f).ToList();
            var degreeOfParallelism = Environment.ProcessorCount;
            var steps = (int)Math.Ceiling((1.0m*tempFiles.Count) / degreeOfParallelism);
            Console.WriteLine($"Starting to sort in {steps} steps.");
            for(int i=0; i<steps; i++)
            {
                var group = ordered.Skip(i*degreeOfParallelism).Take(degreeOfParallelism).ToList();
                await Parallel.ForEachAsync(group,
                    new ParallelOptions() {MaxDegreeOfParallelism = Environment.ProcessorCount},
                    async (tuple,token) =>
                    {
                        var sortedFile = await SortFile(tuple);
                        sortedFiles.Add(sortedFile);
                    });
                    // Trzeba zasynchronizować merge 
                    var partialMerge = $"{outputDirectory}/{Path.GetRandomFileName()}";
                    MergeFiles(sortedFiles, partialMerge);
                    result.Add((sortedFiles[0].Item1, partialMerge));
                    sortedFiles.Clear();
            }
            return result;
        }

        async Task<(char,string)> SortFile(string fileName)
        {
            var lines =(await File.ReadAllLinesAsync(fileName)).Select(LineItem.Parse).ToList();
            var letter = lines[0].Text[0];
            var sortedLines = SortItems(lines);
            string tempFile = $"{outputDirectory}/{Path.GetRandomFileName()}";
            await File.WriteAllLinesAsync(tempFile, sortedLines.Select(l => l.ToString()));
            File.Delete(fileName);
            return (letter,tempFile);
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
                    File.Delete(filePath);
                }
            }
        }
    }
}
