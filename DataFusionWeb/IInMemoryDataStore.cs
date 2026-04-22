using System.Collections.Generic;
using DataFusionArenaWeb.Models;

namespace DataFusionArenaWeb.Services
{
    // Almacén simple en memoria para datasets dinámicos.
    public interface IInMemoryDataStore
    {
        List<Dictionary<string, object?>> Items { get; }
        List<Dictionary<string, object?>> OriginalOrder { get; }
        void SetItems(IEnumerable<Dictionary<string, object?>> items);
        void Clear();
    }

    public class InMemoryDataStore : IInMemoryDataStore
    {
        private readonly List<Dictionary<string, object?>> _items = new();
        private readonly List<Dictionary<string, object?>> _original = new();

        public List<Dictionary<string, object?>> Items => _items;
        public List<Dictionary<string, object?>> OriginalOrder => _original;

        public void SetItems(IEnumerable<Dictionary<string, object?>> items)
        {
            _items.Clear();
            _items.AddRange(items ?? System.Linq.Enumerable.Empty<Dictionary<string, object?>>());

            _original.Clear();
            _original.AddRange(_items.Select(i => new Dictionary<string, object?>(i, System.StringComparer.OrdinalIgnoreCase)));
        }

        public void Clear()
        {
            _items.Clear();
            _original.Clear();
        }
    }
}