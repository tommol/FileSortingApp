using TomaszMolis.FileSort.Sorter.Sorting;

ISorter sorter = new FileSort();
TimeSpan elapsed = await sorter.SortAsync("10000mb.txt", "result_10GB.txt");
Console.WriteLine($"Sorting took {elapsed}.");

