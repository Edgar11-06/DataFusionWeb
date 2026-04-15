using System;
using System.Collections.Generic;
using System.Linq;
using DataFusionArenaWeb.Models;

namespace DataFusionArenaWeb.Services
{
    public class DataStoreRouter : IInMemoryDataStore
    {
        private readonly InMemoryDataStore _inMemory;
        private readonly ConnectionStringProvider _provider;
        private readonly SqlDataStore _sqlStore;
        private readonly PostgresDataStore _pgStore;
        private readonly MariaDataStore _mariaStore;

        public DataStoreRouter(InMemoryDataStore inMemory, ConnectionStringProvider provider)
        {
            _inMemory = inMemory ?? throw new ArgumentNullException(nameof(inMemory));
            _provider = provider ?? throw new ArgumentNullException(nameof(provider));
            _sqlStore = new SqlDataStore(provider);
            _pgStore = new PostgresDataStore(provider);
            _mariaStore = new MariaDataStore(provider);
        }

        private IInMemoryDataStore ActiveStore
        {
            get
            {
                if (!_provider.IsSet) return _inMemory;
                return _provider.Provider switch
                {
                    DatabaseProvider.Postgres => _pgStore,
                    DatabaseProvider.MariaDb  => _mariaStore,
                    _ => _sqlStore,
                };
            }
        }

        public List<DataItem> Items => ActiveStore.Items;
        public List<DataItem> OriginalOrder => ActiveStore.OriginalOrder;
        public void SetItems(IEnumerable<DataItem> items) => ActiveStore.SetItems(items);
        public void Clear() => ActiveStore.Clear();
    }
}
