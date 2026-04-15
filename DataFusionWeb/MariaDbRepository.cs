using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using MySqlConnector;
using DataFusionArenaWeb.Models;

namespace DataFusionArenaWeb.Services
{
    public sealed class MariaDbRepository
    {
        private readonly string _connectionString;
        private readonly string _databaseName;

        public MariaDbRepository(string connectionString, string databaseName = "datafusiondb")
        {
            if (string.IsNullOrWhiteSpace(connectionString)) throw new ArgumentNullException(nameof(connectionString));
            _connectionString = connectionString;
            _databaseName = string.IsNullOrWhiteSpace(databaseName) ? "datafusiondb" : databaseName;
        }

        public async Task<(bool Success, string Message)> TestConnectionAsync(string? overrideConnectionString = null, int timeoutSeconds = 15)
        {
            try
            {
                var csb = new MySqlConnectionStringBuilder(overrideConnectionString ?? _connectionString)
                {
                    Database = "mysql",
                    ConnectionTimeout = (uint)timeoutSeconds
                };
                await using var conn = new MySqlConnection(csb.ConnectionString);
                await conn.OpenAsync();
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = "SELECT 1";
                await cmd.ExecuteScalarAsync();
                return (true, "Conexión OK");
            }
            catch (Exception ex) { return (false, ex.Message); }
        }

        public async Task InitializeAsync()
        {
            // Conectar a 'mysql' para crear DB si no existe
            var builder = new MySqlConnectionStringBuilder(_connectionString) { Database = "mysql" };
            await using (var conn = new MySqlConnection(builder.ConnectionString))
            {
                await conn.OpenAsync();

                // CREATE DATABASE IF NOT EXISTS `db`;
                var createDbSql = $"CREATE DATABASE IF NOT EXISTS `{_databaseName}` CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;";
                await using var cmdCreate = new MySqlCommand(createDbSql, conn);
                await cmdCreate.ExecuteNonQueryAsync();
            }

            // Crear tabla en la base creada
            builder = new MySqlConnectionStringBuilder(_connectionString) { Database = _databaseName };
            await using var conn2 = new MySqlConnection(builder.ConnectionString);
            await conn2.OpenAsync();

            var createTable = @"
CREATE TABLE IF NOT EXISTS `items` (
    `Id` VARCHAR(100) NOT NULL PRIMARY KEY,
    `Nombre` VARCHAR(200),
    `Categoria` VARCHAR(100),
    `Cantidad` INT,
    `PrecioUnitario` DECIMAL(18,4),
    `Valor` DECIMAL(18,4)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;";
            await using var cmd2 = new MySqlCommand(createTable, conn2);
            await cmd2.ExecuteNonQueryAsync();
        }

        public async Task<List<DataItem>> LoadItemsAsync()
        {
            var result = new List<DataItem>();
            var builder = new MySqlConnectionStringBuilder(_connectionString) { Database = _databaseName };
            await using var conn = new MySqlConnection(builder.ConnectionString);
            await conn.OpenAsync();

            var q = "SELECT Id, Nombre, Categoria, Cantidad, PrecioUnitario, Valor FROM `items`;";
            await using var cmd = new MySqlCommand(q, conn);
            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                result.Add(new DataItem
                {
                    Id = reader.IsDBNull(0) ? string.Empty : reader.GetString(0),
                    Nombre = reader.IsDBNull(1) ? string.Empty : reader.GetString(1),
                    Categoria = reader.IsDBNull(2) ? string.Empty : reader.GetString(2),
                    Cantidad = reader.IsDBNull(3) ? 0 : reader.GetInt32(3),
                    PrecioUnitario = reader.IsDBNull(4) ? 0m : reader.GetDecimal(4),
                    Valor = reader.IsDBNull(5) ? 0m : reader.GetDecimal(5)
                });
            }
            return result;
        }

        public async Task SaveItemsAsync(IEnumerable<DataItem> items)
        {
            if (items == null) return;
            var builder = new MySqlConnectionStringBuilder(_connectionString) { Database = _databaseName };
            await using var conn = new MySqlConnection(builder.ConnectionString);
            await conn.OpenAsync();

            await using var tran = await conn.BeginTransactionAsync();

            var sql = @"
INSERT INTO `items` (Id, Nombre, Categoria, Cantidad, PrecioUnitario, Valor)
VALUES (@Id, @Nombre, @Categoria, @Cantidad, @PrecioUnitario, @Valor)
ON DUPLICATE KEY UPDATE
    Nombre = VALUES(Nombre),
    Categoria = VALUES(Categoria),
    Cantidad = VALUES(Cantidad),
    PrecioUnitario = VALUES(PrecioUnitario),
    Valor = VALUES(Valor);";

            await using var cmd = new MySqlCommand(sql, conn, tran);
            cmd.Parameters.Add(new MySqlParameter("@Id", MySqlDbType.VarChar));
            cmd.Parameters.Add(new MySqlParameter("@Nombre", MySqlDbType.VarChar));
            cmd.Parameters.Add(new MySqlParameter("@Categoria", MySqlDbType.VarChar));
            cmd.Parameters.Add(new MySqlParameter("@Cantidad", MySqlDbType.Int32));
            cmd.Parameters.Add(new MySqlParameter("@PrecioUnitario", MySqlDbType.Decimal));
            cmd.Parameters.Add(new MySqlParameter("@Valor", MySqlDbType.Decimal));

            try
            {
                foreach (var it in items)
                {
                    cmd.Parameters[0].Value = it?.Id ?? string.Empty;
                    cmd.Parameters[1].Value = it?.Nombre ?? string.Empty;
                    cmd.Parameters[2].Value = it?.Categoria ?? string.Empty;
                    cmd.Parameters[3].Value = it?.Cantidad ?? 0;
                    cmd.Parameters[4].Value = it?.PrecioUnitario ?? 0m;
                    cmd.Parameters[5].Value = it?.Valor ?? 0m;
                    await cmd.ExecuteNonQueryAsync();
                }

                await tran.CommitAsync();
            }
            catch
            {
                try { await tran.RollbackAsync(); } catch { }
                throw;
            }
        }

        public async Task ClearAsync()
        {
            var builder = new MySqlConnectionStringBuilder(_connectionString) { Database = _databaseName };
            await using var conn = new MySqlConnection(builder.ConnectionString);
            await conn.OpenAsync();
            await using var cmd = new MySqlCommand("TRUNCATE TABLE `items`;", conn);
            await cmd.ExecuteNonQueryAsync();
        }
    }
}