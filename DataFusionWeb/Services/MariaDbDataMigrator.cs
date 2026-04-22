using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using MySqlConnector;

namespace DataFusionArenaWeb.Services
{
    /// <summary>
    /// Migra datasets dinámicos (List&lt;Dictionary&lt;string, object?&gt;&gt;) a MariaDB/MySQL.
    /// Crea la base y la tabla si es necesario; usa TEXT para columnas.
    /// </summary>
    public sealed class MariaDbDataMigrator
    {
        private readonly ConnectionStringProvider _provider;

        public MariaDbDataMigrator(ConnectionStringProvider provider)
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

        private static string MySqlQuote(string id) => $"`{id.Replace("`", "``")}`";

        /// <summary>
        /// Migra rows a MariaDB en la tabla indicada (si no existe, se crea con columnas TEXT).
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

            var mb = new MySqlConnectionStringBuilder(_provider.ConnectionString!);
            var targetDb = string.IsNullOrWhiteSpace(mb.Database) ? "datafusiondb" : mb.Database;

            // connect to server to create database if needed
            var adminCs = new MySqlConnectionStringBuilder(_provider.ConnectionString!) { Database = "mysql" };
            await using (var adminConn = new MySqlConnection(adminCs.ConnectionString))
            {
                await adminConn.OpenAsync(ct);
                var createDbSql = $"CREATE DATABASE IF NOT EXISTS `{targetDb}` CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;";
                await using var createCmd = new MySqlCommand(createDbSql, adminConn);
                await createCmd.ExecuteNonQueryAsync(ct);
            }

            // connect to target DB
            var targetCs = new MySqlConnectionStringBuilder(_provider.ConnectionString!) { Database = targetDb };
            await using var conn = new MySqlConnection(targetCs.ConnectionString);
            await conn.OpenAsync(ct);

            // create table if not exists with TEXT columns
            var colsDef = string.Join(",\n    ", allKeys.Select(k => $"{MySqlQuote(k)} TEXT NULL"));
            var createTableSql = $@"CREATE TABLE IF NOT EXISTS {MySqlQuote(tableName)} (
    {colsDef}
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;";
            await using (var createTableCmd = new MySqlCommand(createTableSql, conn))
            {
                await createTableCmd.ExecuteNonQueryAsync(ct);
            }

            // build insert
            var columnList = allKeys.ToList();
            var columnSql = string.Join(", ", columnList.Select(c => MySqlQuote(c)));
            var paramList = columnList.Select((c, i) => $"@p{i}").ToList();
            var insertSql = $"INSERT INTO {MySqlQuote(tableName)} ({columnSql}) VALUES ({string.Join(", ", paramList)});";

            using var tran = await conn.BeginTransactionAsync(ct);
            try
            {
                await using var insertCmd = new MySqlCommand(insertSql, conn, tran);
                insertCmd.Parameters.Clear();
                for (int i = 0; i < columnList.Count; i++)
                {
                    insertCmd.Parameters.Add(new MySqlParameter($"@p{i}", MySqlDbType.Text) { Value = DBNull.Value });
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