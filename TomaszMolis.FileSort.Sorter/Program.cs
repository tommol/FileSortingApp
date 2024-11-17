using TomaszMolis.FileSort.Sorter.Sorting;

ISorter sorter = new FileSort();
TimeSpan elapsed = await sorter.SortAsync("1000mb.txt", "result_1000MB.txt");
Console.WriteLine($"Sorting took {elapsed}.");

