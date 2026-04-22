using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace DataFusionArenaWeb.Services
{
    /// <summary>
    /// Migra datasets dinámicos (List&lt;Dictionary&lt;string, object?&gt;&gt;) a SQL Server.
    /// Crea la base y la tabla si es necesario; usa NVARCHAR(MAX) para columnas.
    /// </summary>
    public sealed class SqlServerDataMigrator
    {
        private readonly ConnectionStringProvider _provider;

        public SqlServerDataMigrator(ConnectionStringProvider provider)
        {
            _provider = provider ?? throw new ArgumentNullException(nameof(provider));
            if (string.IsNullOrWhiteSpace(_provider.ConnectionString))
                throw new InvalidOperationException("ConnectionStringProvider no tiene cadena de conexión.");
        }

        private static string SanitizeName(string name)
        {
            if (string.IsNullOrWhiteSpace(name)) name = "Table";
            // mantener solo letras/dígitos/underscore
            var s = Regex.Replace(name, @"[^\p{L}\p{N}_]+", "_");
            if (char.IsDigit(s.FirstOrDefault())) s = "t_" + s;
            if (s.Length > 100) s = s.Substring(0, 100);
            return s;
        }

        private static string SqlQuote(string identifier) => $"[{identifier.Replace("]", "]]")}]";

        /// <summary>
        /// Migra los rows a SQL Server en la tabla indicada (si no existe, se crea con NVARCHAR(MAX) por columna).
        /// </summary>
        public async Task<int> MigrateAsync(IEnumerable<Dictionary<string, object?>> rows, string? rawTableName = null, CancellationToken ct = default)
        {
            if (rows == null) return 0;
            var list = rows.ToList();
            if (list.Count == 0) return 0;

            var tableName = SanitizeName(rawTableName ?? $"Imported_{DateTime.UtcNow:yyyyMMdd_HHmmss}");
            var allKeys = list.SelectMany(d => d.Keys).Distinct(StringComparer.OrdinalIgnoreCase).ToList();
            if (allKeys.Count == 0) return 0;

            // connection string parsing
            var csb = new SqlConnectionStringBuilder(_provider.ConnectionString!);
            var targetDb = string.IsNullOrWhiteSpace(csb.InitialCatalog) ? "DataFusionDb" : csb.InitialCatalog;

            // Aumentar timeout de conexión por si el servidor tarda en responder
            const int connectTimeoutSeconds = 60;

            // connect to master to create database if needed
            var masterCsb = new SqlConnectionStringBuilder(_provider.ConnectionString!) { InitialCatalog = "master" };
            masterCsb.ConnectTimeout = connectTimeoutSeconds;
            await using (var masterConn = new SqlConnection(masterCsb.ConnectionString))
            {
                try
                {
                    await masterConn.OpenAsync(ct);
                }
                catch (Exception ex)
                {
                    throw new InvalidOperationException($"No se pudo conectar a SQL Server en fase 'master' con connection string '{masterCsb.DataSource}'. Revisa servidor, puerto, credenciales y firewall. Detalle: {ex.Message}", ex);
                }

                var createDbSql = $@"
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = '{targetDb}')
BEGIN
    CREATE DATABASE [{targetDb}];
END";
                await using var createCmd = new SqlCommand(createDbSql, masterConn);
                await createCmd.ExecuteNonQueryAsync(ct);
            }

            // now connect to target DB
            csb.InitialCatalog = targetDb;
            csb.ConnectTimeout = connectTimeoutSeconds;
            await using var conn = new SqlConnection(csb.ConnectionString);
            try
            {
                await conn.OpenAsync(ct);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"No se pudo conectar a la base de datos objetivo '{targetDb}'. Revisa la cadena de conexión y que el servidor acepte conexiones. Detalle: {ex.Message}", ex);
            }

            // create table if not exists with NVARCHAR(MAX) columns
            var colsDef = string.Join(",\n    ", allKeys.Select(k => $"{SqlQuote(k)} NVARCHAR(MAX) NULL"));
            var createTableSql = $@"
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'{tableName}') AND type in (N'U'))
BEGIN
    CREATE TABLE {SqlQuote(tableName)} (
        {colsDef}
    );
END";
            await using (var createTableCmd = new SqlCommand(createTableSql, conn))
            {
                await createTableCmd.ExecuteNonQueryAsync(ct);
            }

            // build insert
            var columnList = allKeys.ToList();
            var columnSql = string.Join(", ", columnList.Select(c => SqlQuote(c)));
            var paramList = columnList.Select((c, i) => $"@p{i}").ToList();
            var insertSql = $"INSERT INTO {SqlQuote(tableName)} ({columnSql}) VALUES ({string.Join(", ", paramList)});";

            using var tran = conn.BeginTransaction();
            try
            {
                using var insertCmd = conn.CreateCommand();
                insertCmd.Transaction = tran;
                insertCmd.CommandText = insertSql;
                insertCmd.Parameters.Clear();

                // create parameters (NVARCHAR(MAX) by default)
                for (int i = 0; i < columnList.Count; i++)
                {
                    var p = new SqlParameter(paramList[i], System.Data.SqlDbType.NVarChar, -1) { Value = DBNull.Value };
                    insertCmd.Parameters.Add(p);
                }

                int inserted = 0;
                foreach (var row in list)
                {
                    if (ct.IsCancellationRequested) break;
                    for (int i = 0; i < columnList.Count; i++)
                    {
                        var key = columnList[i];
                        if (row != null && row.TryGetValue(key, out var v) && v != null)
                        {
                            // convert value to string to avoid type issues; repositorio puede evolucionar a tipos inferidos
                            insertCmd.Parameters[i].Value = v is string ? (object)v : v.ToString()!;
                        }
                        else
                        {
                            insertCmd.Parameters[i].Value = DBNull.Value;
                        }
                    }

                    await insertCmd.ExecuteNonQueryAsync(ct);
                    inserted++;
                }

                tran.Commit();
                return inserted;
            }
            catch
            {
                try { tran.Rollback(); } catch { }
                throw;
            }
        }

        /// <summary>
        /// Convierte la colección dinámica a DataTable y llama a MigrateAsync.
        /// </summary>
        public Task<int> MigrateAsDataTableAsync(IEnumerable<Dictionary<string, object?>> rows, string? rawTableName = null, CancellationToken ct = default)
        {
            return MigrateAsync(rows, rawTableName, ct);
        }
    }
}