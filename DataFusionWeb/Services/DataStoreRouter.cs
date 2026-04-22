using System;
using System.Collections.Generic;

namespace DataFusionArenaWeb.Services
{
    // Router sencillo: delega en el almacén en memoria.
    // Evita dependencias directas a stores SQL legados.
    public class DataStoreRouter : IInMemoryDataStore
    {
        private readonly IInMemoryDataStore _inMemory;

        public DataStoreRouter(IInMemoryDataStore inMemory)
        {
            _inMemory = inMemory ?? throw new ArgumentNullException(nameof(inMemory));
        }

        public List<Dictionary<string, object?>> Items => _inMemory.Items;
        public List<Dictionary<string, object?>> OriginalOrder => _inMemory.OriginalOrder;
        public void SetItems(IEnumerable<Dictionary<string, object?>> items) => _inMemory.SetItems(items);
        public void Clear() => _inMemory.Clear();
    }
}
