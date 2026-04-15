using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Npgsql;
using DataFusionArenaWeb.Models;

namespace DataFusionArenaWeb.Services
{
    public sealed class PostgresRepository
    {
        private readonly string _connectionString;
        private readonly string _databaseName;

        public PostgresRepository(string connectionString, string databaseName = "datafusiondb")
        {
            if (string.IsNullOrWhiteSpace(connectionString)) throw new ArgumentNullException(nameof(connectionString));
            _connectionString = connectionString;
            _databaseName = string.IsNullOrWhiteSpace(databaseName) ? "datafusiondb" : databaseName;
        }

        public async Task<(bool Success, string Message)> TestConnectionAsync(string? overrideConnectionString = null, int timeoutSeconds = 15)
        {
            try
            {
                var csb = new NpgsqlConnectionStringBuilder(overrideConnectionString ?? _connectionString) { Database = "postgres", Timeout = timeoutSeconds };
                await using var conn = new NpgsqlConnection(csb.ConnectionString);
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
            var builder = new NpgsqlConnectionStringBuilder(_connectionString) { Database = "postgres" };
            await using (var conn = new NpgsqlConnection(builder.ConnectionString))
            {
                await conn.OpenAsync();

                // crear base si no existe
                var checkSql = "SELECT 1 FROM pg_database WHERE datname = @db";
                await using (var check = new NpgsqlCommand(checkSql, conn))
                {
                    check.Parameters.AddWithValue("@db", _databaseName);
                    var exists = await check.ExecuteScalarAsync();
                    if (exists == null)
                    {
                        await using var create = new NpgsqlCommand($"CREATE DATABASE \"{_databaseName}\";", conn);
                        await create.ExecuteNonQueryAsync();
                    }
                }
            }

            // crear tabla si no existe en la DB destino
            builder = new NpgsqlConnectionStringBuilder(_connectionString) { Database = _databaseName };
            await using var conn2 = new NpgsqlConnection(builder.ConnectionString);
            await conn2.OpenAsync();

            var createTable = @"
CREATE TABLE IF NOT EXISTS public.items (
    id TEXT PRIMARY KEY,
    nombre TEXT,
    categoria TEXT,
    cantidad INTEGER,
    preciounitario NUMERIC(18,4),
    valor NUMERIC(18,4)
);";
            await using var cmd2 = new NpgsqlCommand(createTable, conn2);
            await cmd2.ExecuteNonQueryAsync();
        }

        public async Task<List<DataItem>> LoadItemsAsync()
        {
            var result = new List<DataItem>();
            var builder = new NpgsqlConnectionStringBuilder(_connectionString) { Database = _databaseName };
            await using var conn = new NpgsqlConnection(builder.ConnectionString);
            await conn.OpenAsync();

            var q = "SELECT id, nombre, categoria, cantidad, preciounitario, valor FROM public.items;";
            await using var cmd = new NpgsqlCommand(q, conn);
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
            var builder = new NpgsqlConnectionStringBuilder(_connectionString) { Database = _databaseName };
            await using var conn = new NpgsqlConnection(builder.ConnectionString);
            await conn.OpenAsync();

            await using var tran = await conn.BeginTransactionAsync();
            var sql = @"
INSERT INTO public.items (id, nombre, categoria, cantidad, preciounitario, valor)
VALUES (@id, @nombre, @categoria, @cantidad, @preciounitario, @valor)
ON CONFLICT (id) DO UPDATE
SET nombre = EXCLUDED.nombre, categoria = EXCLUDED.categoria, cantidad = EXCLUDED.cantidad, preciounitario = EXCLUDED.preciounitario, valor = EXCLUDED.valor;";

            await using var cmd = new NpgsqlCommand(sql, conn, tran);
            cmd.Parameters.Add(new NpgsqlParameter("@id", NpgsqlTypes.NpgsqlDbType.Text));
            cmd.Parameters.Add(new NpgsqlParameter("@nombre", NpgsqlTypes.NpgsqlDbType.Text));
            cmd.Parameters.Add(new NpgsqlParameter("@categoria", NpgsqlTypes.NpgsqlDbType.Text));
            cmd.Parameters.Add(new NpgsqlParameter("@cantidad", NpgsqlTypes.NpgsqlDbType.Integer));
            cmd.Parameters.Add(new NpgsqlParameter("@preciounitario", NpgsqlTypes.NpgsqlDbType.Numeric));
            cmd.Parameters.Add(new NpgsqlParameter("@valor", NpgsqlTypes.NpgsqlDbType.Numeric));

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
            var builder = new NpgsqlConnectionStringBuilder(_connectionString) { Database = _databaseName };
            await using var conn = new NpgsqlConnection(builder.ConnectionString);
            await conn.OpenAsync();
            await using var cmd = new NpgsqlCommand("TRUNCATE TABLE public.items;", conn);
            await cmd.ExecuteNonQueryAsync();
        }
    }
}