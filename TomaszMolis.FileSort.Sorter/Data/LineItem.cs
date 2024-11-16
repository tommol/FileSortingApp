using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TomaszMolis.FileSort.Sorter.Data
{
    public record LineItem : IComparable<LineItem>
    {
        public long Number { get; init; }
        public string Text { get; init; }

        public LineItem(long number, string text)
        {
            Number = number;
            Text = text;
        }

        public int CompareTo(LineItem other)
        {
            var stringComparison = string.CompareOrdinal(Text, other.Text);
            if (stringComparison != 0)
            {
                return Number.CompareTo(other.Number);
            }
            return stringComparison;
        }

        public override string ToString()
        {
            return $"{Number}. {Text}";
        }

        public static LineItem Parse(string line)
        {
            if (line == null)
            {
                return null;
            }
            var parts = line.Split(". ", 2);
            if (parts.Length != 2)
            {
                throw new FormatException("Invalid line format");
            }

            long number;
            if (!long.TryParse(parts[0], out number))
            {
                throw new FormatException("Invalid number format");
            }

            return new LineItem(number, parts[1]);
        }
    }   
}
