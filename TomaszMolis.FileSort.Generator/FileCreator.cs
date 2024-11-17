namespace TomaszMolis.FileSort.Generator;

public sealed class FileCreator
{
    private readonly Random random = new();
    
    public async Task<string> Create(long lenght)
    {
        var outputPath = Path.GetRandomFileName();
        var dictionary = (await LoadDictionary()).ToArray();
        var dictionarySize = dictionary.Length;
        int numberPart = 0;
        string text = string.Empty;
        long fileSize = 0;
        await using var writer = new StreamWriter(outputPath, append: true);
        do
        {
            numberPart = random.Next();
            text = dictionary[random.Next(dictionarySize - 1)];
            var line = $"{numberPart}. {text}";
            fileSize += line.Length;
            await writer.WriteLineAsync(line);
        } while (fileSize < lenght);
        return outputPath;
    }

    private async Task<IEnumerable<string>> LoadDictionary()
    {
        return await File.ReadAllLinesAsync("dictionary.txt");
    }
}