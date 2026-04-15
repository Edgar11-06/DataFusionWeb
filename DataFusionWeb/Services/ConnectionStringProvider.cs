using System;

namespace DataFusionArenaWeb.Services
{
    public enum DatabaseProvider { SqlServer, Postgres, MariaDb }

    public class ConnectionStringProvider
    {
        private string? _connectionString;
        private DatabaseProvider _provider = DatabaseProvider.SqlServer;

        public string? ConnectionString => _connectionString;
        public DatabaseProvider Provider => _provider;
        public bool IsSet => !string.IsNullOrWhiteSpace(_connectionString);

        public void Set(string connectionString, DatabaseProvider provider = DatabaseProvider.SqlServer)
        {
            if (string.IsNullOrWhiteSpace(connectionString)) throw new ArgumentNullException(nameof(connectionString));
            _connectionString = connectionString;
            _provider = provider;
        }

        public void Clear()
        {
            _connectionString = null;
            _provider = DatabaseProvider.SqlServer;
        }
    }
}