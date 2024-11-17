using TomaszMolis.FileSort.Sorter.Sorting;

ISorter sorter = new FileSort();
TimeSpan elapsed = await sorter.SortAsync("testdata.txt", "results/test_1.txt");
Console.WriteLine($"Sorting took {elapsed}.");

