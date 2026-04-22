using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace DataFusionArenaWeb.Services
{
    /// <summary>
    /// Migra datasets dinámicos (List&lt;Dictionary&lt;string, object?&gt;&gt;) a PostgreSQL.
    /// Crea la base y la tabla si es necesario; usa TEXT para columnas.
    /// </summary>
    public sealed class PostgresDataMigrator
    {
        private readonly ConnectionStringProvider _provider;

        public PostgresDataMigrator(ConnectionStringProvider provider)
        {
            _provider = provider ?? throw new ArgumentNullException(nameof(provider));
            if (string.IsNullOrWhiteSpace(_provider.ConnectionString))
                throw new InvalidOperationException("ConnectionStringProvider no tiene cadena de conexión.");
        }

        private static string SanitizeName(string name)
        {
            if (string.IsNullOrWhiteSpace(name)) name = "table";
            var s = Regex.Replace(name, @"[^\p{L}\p{N}_]+", "_");
            if (char.IsDigit(s.FirstOrDefault())) s = "t_" + s;
            if (s.Length > 100) s = s.Substring(0, 100);
            return s;
        }

        private static string PgQuote(string id) => $"\"{id.Replace("\"", "\"\"")}\"";

        /// <summary>
        /// Migra rows a PostgreSQL en la tabla indicada (si no existe, se crea con columnas TEXT).
        /// Devuelve número de filas insertadas.
        /// </summary>
        public async Task<int> MigrateAsync(IEnumerable<Dictionary<string, object?>> rows, string? rawTableName = null, CancellationToken ct = default)
        {
            if (rows == null) return 0;
            var list = rows.ToList();
            if (list.Count == 0) return 0;

            var tableName = SanitizeName(rawTableName ?? $"imported_{DateTime.UtcNow:yyyyMMdd_HHmmss}");
            var allKeys = list.SelectMany(d => d.Keys).Distinct(StringComparer.OrdinalIgnoreCase).ToList();
            if (allKeys.Count == 0) return 0;

            var nb = new NpgsqlConnectionStringBuilder(_provider.ConnectionString!);
            var targetDb = string.IsNullOrWhiteSpace(nb.Database) ? "datafusiondb" : nb.Database;

            // connect to default 'postgres' DB to create database if needed
            var adminCs = new NpgsqlConnectionStringBuilder(_provider.ConnectionString!) { Database = "postgres" };
            await using (var adminConn = new NpgsqlConnection(adminCs.ConnectionString))
            {
                await adminConn.OpenAsync(ct);
                var checkSql = $"SELECT 1 FROM pg_database WHERE datname = '{targetDb}'";
                await using (var checkCmd = new NpgsqlCommand(checkSql, adminConn))
                {
                    var exists = await checkCmd.ExecuteScalarAsync(ct);
                    if (exists == null)
                    {
                        var createDb = $"CREATE DATABASE \"{targetDb}\";";
                        await using var createCmd = new NpgsqlCommand(createDb, adminConn);
                        await createCmd.ExecuteNonQueryAsync(ct);
                    }
                }
            }

            // connect to target DB
            var targetCs = new NpgsqlConnectionStringBuilder(_provider.ConnectionString!) { Database = targetDb };
            await using var conn = new NpgsqlConnection(targetCs.ConnectionString);
            await conn.OpenAsync(ct);

            // create table if not exists with TEXT columns
            var colsDef = string.Join(",\n    ", allKeys.Select(k => $"{PgQuote(k)} TEXT NULL"));
            var createSql = $@"
CREATE TABLE IF NOT EXISTS {PgQuote(tableName)} (
    {colsDef}
);";
            await using (var createCmd = new NpgsqlCommand(createSql, conn))
            {
                await createCmd.ExecuteNonQueryAsync(ct);
            }

            // build insert
            var columnList = allKeys.ToList();
            var columnSql = string.Join(", ", columnList.Select(c => PgQuote(c)));
            var paramList = columnList.Select((c, i) => $"@p{i}").ToList();
            var insertSql = $"INSERT INTO {PgQuote(tableName)} ({columnSql}) VALUES ({string.Join(", ", paramList)});";

            using var tran = await conn.BeginTransactionAsync(ct);
            try
            {
                await using var insertCmd = new NpgsqlCommand(insertSql, conn, tran);
                insertCmd.Parameters.Clear();
                for (int i = 0; i < columnList.Count; i++)
                {
                    insertCmd.Parameters.Add(new NpgsqlParameter($"@p{i}", NpgsqlTypes.NpgsqlDbType.Text) { Value = DBNull.Value });
                }

                int inserted = 0;
                foreach (var row in list)
                {
                    if (ct.IsCancellationRequested) break;
                    for (int i = 0; i < columnList.Count; i++)
                    {
                        var key = columnList[i];
                        if (row != null && row.TryGetValue(key, out var v) && v != null)
                            insertCmd.Parameters[i].Value = v is string ? (object)v : v.ToString()!;
                        else
                            insertCmd.Parameters[i].Value = DBNull.Value;
                    }

                    await insertCmd.ExecuteNonQueryAsync(ct);
                    inserted++;
                }

                await tran.CommitAsync(ct);
                return inserted;
            }
            catch
            {
                try { await tran.RollbackAsync(ct); } catch { }
                throw;
            }
        }
    }
}