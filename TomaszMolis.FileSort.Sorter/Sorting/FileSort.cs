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

        public TimeSpan Sort(string inputFilePath, string outputFilePath)
        {
            
            FileInfo inputFile = new FileInfo(inputFilePath);
            CHUNK_SIZE = CalculateChunkSize(inputFile.Length);
            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();
            try
            {
                List<string> tempFiles = Split(inputFilePath);
                List<string> sortedFiles = SortFiles(tempFiles);

                MergeSortedFilesAsync(sortedFiles, outputFilePath);
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
                    var tempFile = Path.GetTempFileName();
                    sortedFiles.Add(tempFile);
                    File.WriteAllLines(tempFile, sortedLines.Select(l => l.ToString()));
                });
                tasks.Add(sortTask);
                sortTask.Start();
            }
            Task.WaitAll(tasks.ToArray());
            return sortedFiles;
        }

        static void  WriteSplitted(List<string> chunk, string tempFile)
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


        static void MergeSortedFilesAsync(List<string> tempFiles, string outputFile)
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

                    var nextLine =  readers[minIndex].ReadLine();
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

        static int CalculateChunkSize(long fileSize)
        {
            if(fileSize> 500*1024*1024)
            {
                return 8000000;

            }
            else if(fileSize > 50 * 1024 * 1024)
            {
                return 80000;
            }
            else
            {
                return 8000;
            }
        }

    }
}
