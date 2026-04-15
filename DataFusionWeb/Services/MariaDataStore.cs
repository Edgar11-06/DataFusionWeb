using System;
using System.Collections.Generic;
using System.Linq;
using DataFusionArenaWeb.Models;

namespace DataFusionArenaWeb.Services
{
    public class MariaDataStore : IInMemoryDataStore
    {
        private readonly ConnectionStringProvider _provider;
        private readonly string _databaseName;

        public MariaDataStore(ConnectionStringProvider provider, string databaseName = "datafusiondb")
        {
            _provider = provider ?? throw new ArgumentNullException(nameof(provider));
            _databaseName = string.IsNullOrWhiteSpace(databaseName) ? "datafusiondb" : databaseName;
        }

        private MariaDbRepository CreateRepo()
        {
            if (!_provider.IsSet) throw new InvalidOperationException("La cadena de conexión no está establecida.");
            return new MariaDbRepository(_provider.ConnectionString!, _databaseName);
        }

        public List<DataItem> Items
        {
            get
            {
                var repo = CreateRepo();
                return repo.LoadItemsAsync().GetAwaiter().GetResult();
            }
        }

        public List<DataItem> OriginalOrder => Items.ToList();

        public void SetItems(IEnumerable<DataItem> items)
        {
            var repo = CreateRepo();
            repo.SaveItemsAsync(items ?? Enumerable.Empty<DataItem>()).GetAwaiter().GetResult();
        }

        public void Clear()
        {
            var repo = CreateRepo();
            repo.ClearAsync().GetAwaiter().GetResult();
        }
    }
}