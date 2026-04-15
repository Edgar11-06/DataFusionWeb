using DataFusionArenaWeb.Models;

namespace DataFusionArenaWeb.Services
{
    // Almacén simple en memoria (singleton para pruebas).
    public interface IInMemoryDataStore
    {
        List<DataItem> Items { get; }
        List<DataItem> OriginalOrder { get; }
        void SetItems(IEnumerable<DataItem> items);
        void Clear();
    }

    public class InMemoryDataStore : IInMemoryDataStore
    {
        private readonly List<DataItem> _items = new();
        private readonly List<DataItem> _original = new();

        public List<DataItem> Items => _items;
        public List<DataItem> OriginalOrder => _original;

        public void SetItems(IEnumerable<DataItem> items)
        {
            _items.Clear();
            _items.AddRange(items ?? Enumerable.Empty<DataItem>());

            _original.Clear();
            _original.AddRange(_items.Select(i => i)); // mismas referencias en memoria
        }

        public void Clear()
        {
            _items.Clear();
            _original.Clear();
        }
    }
}