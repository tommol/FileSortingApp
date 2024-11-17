using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TomaszMolis.FileSort.Sorter.Sorting
{
    public interface ISorter
    {
        Task<TimeSpan> SortAsync(string inputFilePath, string outputFilePath);
    }
}
