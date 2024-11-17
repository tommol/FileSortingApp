using System;
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
                List<string> tempFiles = Split(inputFilePath);
                splitting.Stop();
                Console.WriteLine("Splitted into {0} files", tempFiles.Count);
                Console.WriteLine($"Splitting took {splitting.Elapsed}.");
                Stopwatch sorting = new Stopwatch();
                sorting.Start();
                List<string> sortedFiles = SortFiles(tempFiles);
                sorting.Stop();
                Console.WriteLine($"Sorting took {sorting.Elapsed}.");
                Stopwatch merging = new Stopwatch();
                await MergeChunksParallel(sortedFiles, outputFilePath);
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

        List<string> SortFiles(List<string> tempFiles)
        {
            List<string> sortedFiles = new List<string>();
            List<Task> tasks = new List<Task>();
            foreach (var file in tempFiles)
            {
                var sortTask = new Task(() =>
                {
                    var lines = File.ReadAllLines(file).Select(LineItem.Parse).ToList();
                    var sortedLines = SortItems(lines);
                    string tempFile = Path.GetTempFileName();
                    sortedFiles.Add(tempFile);
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

        static void MergeSortedFiles(List<string> tempFiles, string outputFile)
        {
            // Create a list of readers for each temporary file
            List<StreamReader> readers = tempFiles.Select(file => new StreamReader(file)).ToList();
            List<LineItem> currentLines = readers.Select(r => LineItem.Parse(r.ReadLine())).ToList();

            using (StreamWriter writer = new StreamWriter(outputFile))
            {
                while (currentLines.Any(line => line != null))
                {
                    LineItem minLine = null;
                    int minIndex = -1;

                    for (int i = 0; i < currentLines.Count; i++)
                    {
                        if (currentLines[i] != null && (minLine == null || currentLines[i].CompareTo(minLine) < 0))
                        {
                            minLine = currentLines[i];
                            minIndex = i;
                        }
                    }

                    // Write the smallest line to the output file
                    writer.WriteLine(minLine.ToString());

                    var nextLine = readers[minIndex].ReadLine();
                    // Read the next line from the file that provided the smallest line
                    currentLines[minIndex] = LineItem.Parse(nextLine);
                }
            }

            // Close all readers
            foreach (var reader in readers)
            {
                reader.Close();
            }

            // Clean up temporary files
            foreach (var file in tempFiles)
            {
                File.Delete(file);
            }
        }


        private static async Task MergeChunksParallel(List<string> tempFiles, string outputFile)
        {
            int chunkCount = tempFiles.Count;
            int parallelismDegree = Math.Min(Environment.ProcessorCount, chunkCount);

            // Split files into groups for parallel processing
            var chunks = SplitIntoGroups(tempFiles, parallelismDegree);

            var tasks = new List<Task>();
            List<string> outputFiles = new List<string>();

            Parallel.ForEach(chunks, group =>
            {
                var tmpFile = Path.GetTempFileName();
                outputFiles.Add(tmpFile);
                MergeSortedFiles(group, tmpFile);
            });           
            MergeSortedFiles(outputFiles, outputFile);
        }

        // Helper method to merge a set of files in one group
        private static void MergeSortedFilesInGroup(List<string> groupFiles, string outputFile)
        {
            List<StreamReader> readers = groupFiles.Select(file => new StreamReader(file)).ToList();
            List<LineItem> currentLines = readers.Select(reader => LineItem.Parse(reader.ReadLine())).ToList();

            using (var writer = new StreamWriter(outputFile, append: true)) // Append mode for merging
            {
                while (true)
                {
                    // Find the smallest line from the current set of lines
                    LineItem minLine = null;
                    int minIndex = -1;

                    for (int i = 0; i < currentLines.Count; i++)
                    {
                        if (currentLines[i] != null && (minLine == null || currentLines[i].CompareTo(minLine) < 0))
                        {
                            minLine = currentLines[i];
                            minIndex = i;
                        }
                    }

                    if (minLine == null) break; // All lines in this group have been processed

                    writer.WriteLine(minLine);

                    // Read the next line from the corresponding reader
                    currentLines[minIndex] = LineItem.Parse(readers[minIndex].ReadLine());
                }
            }

            // Close all readers
            foreach (var reader in readers)
            {
                reader.Close();
            }
        }

        // Split the files into smaller groups for parallel processing
        private static List<List<string>> SplitIntoGroups(List<string> files, int groupCount)
        {
            var result = new List<List<string>>();
            int filesPerGroup = (int)Math.Ceiling((double)files.Count / groupCount);

            for (int i = 0; i < filesPerGroup; i++)
            {
                var group = files.Skip(i * groupCount).Take(groupCount).ToList();
                result.Add(group);
            }

            return result;
        }
    }
}
