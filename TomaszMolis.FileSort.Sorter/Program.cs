using TomaszMolis.FileSort.Sorter.Sorting;

ISorter sorter = new FileSort();
TimeSpan elapsed = sorter.Sort("testdata.txt", "results/test_5.txt");
Console.WriteLine($"Sorting took {elapsed}.");

