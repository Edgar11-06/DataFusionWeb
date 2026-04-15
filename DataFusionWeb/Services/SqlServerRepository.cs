using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using DataFusionArenaWeb.Models;

namespace DataFusionArenaWeb.Services
{
    public sealed class SqlServerRepository
    {
        private readonly string _connectionString;
        private readonly string _databaseName;

        public SqlServerRepository(string connectionString, string databaseName = "ConexionSQL")
        {
            if (string.IsNullOrWhiteSpace(connectionString)) throw new ArgumentNullException(nameof(connectionString));
            _connectionString = connectionString;
            _databaseName = string.IsNullOrWhiteSpace(databaseName) ? "ConexionSQL" : databaseName;
        }

        // Test de conexión ligero; devuelve (Success, Message)
        public async Task<(bool Success, string Message)> TestConnectionAsync(string? overrideConnectionString = null, int timeoutSeconds = 15)
        {
            try
            {
                var cs = overrideConnectionString ?? _connectionString;
                var builder = new SqlConnectionStringBuilder(cs)
                {
                    InitialCatalog = "master",
                    ConnectTimeout = timeoutSeconds
                };

                await using var conn = new SqlConnection(builder.ConnectionString);
                await conn.OpenAsync();
                await using var cmd = conn.CreateCommand();
                cmd.CommandText = "SELECT 1";
                await cmd.ExecuteScalarAsync();
                return (true, "Conexión OK");
            }
            catch (SqlException ex)
            {
                return (false, $"Error SQL ({ex.Number}): {ex.Message}");
            }
            catch (Exception ex)
            {
                return (false, $"Error de conexión: {ex.Message}");
            }
        }

        public async Task InitializeAsync(int connectTimeoutSeconds = 15)
        {
            var builder = new SqlConnectionStringBuilder(_connectionString)
            {
                InitialCatalog = "master",
                ConnectTimeout = connectTimeoutSeconds
            };

            await using var conn = new SqlConnection(builder.ConnectionString);
            await conn.OpenAsync();

            // Crear base si no existe
            var createDbSql = $@"
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = '{_databaseName}')
BEGIN
    CREATE DATABASE [{_databaseName}];
END";
            await using (var cmd = new SqlCommand(createDbSql, conn))
            {
                await cmd.ExecuteNonQueryAsync();
            }

            // Cambiar a la base creada
            conn.ChangeDatabase(_databaseName);

            // Crear tabla Items si no existe (tipos adecuados para DataItem)
            var createTableSql = @"
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Items]') AND type in (N'U'))
BEGIN
    CREATE TABLE [dbo].[Items](
        [Id] NVARCHAR(100) NOT NULL PRIMARY KEY,
        [Nombre] NVARCHAR(200) NULL,
        [Categoria] NVARCHAR(100) NULL,
        [Cantidad] INT NULL,
        [PrecioUnitario] DECIMAL(18,4) NULL,
        [Valor] DECIMAL(18,4) NULL
    );
END";
            await using (var cmd2 = new SqlCommand(createTableSql, conn))
            {
                await cmd2.ExecuteNonQueryAsync();
            }
        }

        // Cargar todos los items desde la tabla dbo.Items
        public async Task<List<DataItem>> LoadItemsAsync(int connectTimeoutSeconds = 15)
        {
            var result = new List<DataItem>();

            var builder = new SqlConnectionStringBuilder(_connectionString)
            {
                InitialCatalog = _databaseName,
                ConnectTimeout = connectTimeoutSeconds
            };

            await using var conn = new SqlConnection(builder.ConnectionString);
            try
            {
                await conn.OpenAsync();
            }
            catch (SqlException ex)
            {
                throw new InvalidOperationException($"Error al conectar a SQL Server (nº {ex.Number}): {ex.Message}", ex);
            }

            const string q = "SELECT Id, Nombre, Categoria, Cantidad, PrecioUnitario, Valor FROM dbo.Items;";

            await using var cmd = new SqlCommand(q, conn);
            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                var id = !reader.IsDBNull(0) ? reader.GetString(0) : string.Empty;
                var nombre = !reader.IsDBNull(1) ? reader.GetString(1) : string.Empty;
                var categoria = !reader.IsDBNull(2) ? reader.GetString(2) : string.Empty;
                var cantidad = !reader.IsDBNull(3) ? reader.GetInt32(3) : 0;

                decimal precioUnitario = 0m;
                if (!reader.IsDBNull(4))
                {
                    try
                    {
                        precioUnitario = reader.GetDecimal(4);
                    }
                    catch
                    {
                        precioUnitario = Convert.ToDecimal(reader.GetValue(4));
                    }
                }

                decimal valor = 0m;
                if (!reader.IsDBNull(5))
                {
                    try
                    {
                        valor = reader.GetDecimal(5);
                    }
                    catch
                    {
                        valor = Convert.ToDecimal(reader.GetValue(5));
                    }
                }

                result.Add(new DataItem
                {
                    Id = id,
                    Nombre = nombre,
                    Categoria = categoria,
                    Cantidad = cantidad,
                    PrecioUnitario = precioUnitario,
                    Valor = valor
                });
            }

            return result;
        }

        public async Task SaveItemsAsync(IEnumerable<DataItem> items, int connectTimeoutSeconds = 15)
        {
            if (items == null) return;

            var builder = new SqlConnectionStringBuilder(_connectionString) { InitialCatalog = _databaseName, ConnectTimeout = connectTimeoutSeconds };
            await using var conn = new SqlConnection(builder.ConnectionString);

            try
            {
                await conn.OpenAsync();
            }
            catch (SqlException ex)
            {
                // Dejar al llamador el mensaje para mostrar en UI
                throw new InvalidOperationException($"Error relacionado con la red o la instancia SQL. SQL Error {ex.Number}: {ex.Message}", ex);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Error al abrir conexión: {ex.Message}", ex);
            }

            // Usar BeginTransaction síncrono (devuelve SqlTransaction y es compatible)
            using var tran = conn.BeginTransaction();

            using var cmd = conn.CreateCommand();
            cmd.Transaction = tran;
            cmd.CommandText = @"
MERGE INTO dbo.Items AS Target
USING (VALUES (@Id, @Nombre, @Categoria, @Cantidad, @PrecioUnitario, @Valor)) 
       AS Source (Id, Nombre, Categoria, Cantidad, PrecioUnitario, Valor)
ON Target.Id = Source.Id
WHEN MATCHED THEN
    UPDATE SET Nombre = Source.Nombre, Categoria = Source.Categoria, Cantidad = Source.Cantidad, PrecioUnitario = Source.PrecioUnitario, Valor = Source.Valor
WHEN NOT MATCHED THEN
    INSERT (Id, Nombre, Categoria, Cantidad, PrecioUnitario, Valor)
    VALUES (Source.Id, Source.Nombre, Source.Categoria, Source.Cantidad, Source.PrecioUnitario, Source.Valor);";

            // parámetros (se reutilizan y se reasignan por cada item)
            var pId = new SqlParameter("@Id", SqlDbType.NVarChar, 100);
            var pNombre = new SqlParameter("@Nombre", SqlDbType.NVarChar, 200);
            var pCategoria = new SqlParameter("@Categoria", SqlDbType.NVarChar, 100);
            var pCantidad = new SqlParameter("@Cantidad", SqlDbType.Int);
            var pPrecio = new SqlParameter("@PrecioUnitario", SqlDbType.Decimal) { Precision = 18, Scale = 4 };
            var pValor = new SqlParameter("@Valor", SqlDbType.Decimal) { Precision = 18, Scale = 4 };

            cmd.Parameters.Add(pId);
            cmd.Parameters.Add(pNombre);
            cmd.Parameters.Add(pCategoria);
            cmd.Parameters.Add(pCantidad);
            cmd.Parameters.Add(pPrecio);
            cmd.Parameters.Add(pValor);

            try
            {
                foreach (var it in items)
                {
                    pId.Value = it?.Id ?? string.Empty;
                    pNombre.Value = it?.Nombre ?? string.Empty;
                    pCategoria.Value = it?.Categoria ?? string.Empty;
                    pCantidad.Value = it?.Cantidad ?? 0;
                    pPrecio.Value = it?.PrecioUnitario ?? 0m;
                    pValor.Value = it?.Valor ?? 0m;

                    await cmd.ExecuteNonQueryAsync();
                }

                tran.Commit();
            }
            catch (SqlException ex)
            {
                try { tran.Rollback(); } catch { /* ignorar */ }
                throw new InvalidOperationException($"Error de SQL ({ex.Number}): {ex.Message}", ex);
            }
            catch
            {
                try { tran.Rollback(); } catch { /* ignorar */ }
                throw;
            }
        }

        // Elimina todos los registros de la tabla Items
        public async Task ClearAsync(int connectTimeoutSeconds = 15)
        {
            var builder = new SqlConnectionStringBuilder(_connectionString) { InitialCatalog = _databaseName, ConnectTimeout = connectTimeoutSeconds };
            await using var conn = new SqlConnection(builder.ConnectionString);
            await conn.OpenAsync();

            using var tran = conn.BeginTransaction();
            using var cmd = conn.CreateCommand();
            cmd.Transaction = tran;
            cmd.CommandText = "DELETE FROM dbo.Items;";
            try
            {
                await cmd.ExecuteNonQueryAsync();
                tran.Commit();
            }
            catch
            {
                try { tran.Rollback(); } catch { }
                throw;
            }
        }
    }
}