using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using System.Threading.Tasks;
using DataFusionArenaWeb.Services;
using Microsoft.Data.SqlClient;
using Npgsql;
using MySqlConnector;

namespace DataFusionArenaWeb.Pages.Connection
{
    public class IndexModel : PageModel
    {
        private readonly ConnectionStringProvider _provider;

        public IndexModel(ConnectionStringProvider provider) => _provider = provider;

        [BindProperty] public string ConnectionString { get; set; } = string.Empty;
        [BindProperty] public DatabaseProvider Provider { get; set; } = DatabaseProvider.SqlServer;
        public string? Message { get; set; }

        public void OnGet()
        {
            ConnectionString = _provider.ConnectionString ?? string.Empty;
            Provider = _provider.Provider;
        }

        public async Task<IActionResult> OnPostAsync()
        {
            if (string.IsNullOrWhiteSpace(ConnectionString))
            {
                ModelState.AddModelError(nameof(ConnectionString), "La cadena de conexión no puede estar vacía.");
                return Page();
            }

            _provider.Set(ConnectionString, Provider);

            try
            {
                if (Provider == DatabaseProvider.SqlServer)
                {
                    // Conectar a 'master' y crear base si no existe
                    var csb = new SqlConnectionStringBuilder(ConnectionString) { InitialCatalog = "master" };
                    await using var conn = new SqlConnection(csb.ConnectionString);
                    await conn.OpenAsync();
                    var targetDb = "DataFusionDb";
                    var createDbSql = $@"
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = '{targetDb}')
BEGIN
    CREATE DATABASE [{targetDb}];
END";
                    await using (var cmd = new SqlCommand(createDbSql, conn))
                    {
                        await cmd.ExecuteNonQueryAsync();
                    }

                    Message = "Cadena aceptada e inicialización completada (SQL Server).";
                }
                else if (Provider == DatabaseProvider.Postgres)
                {
                    // Conectar a 'postgres' para crear la base si no existe
                    var nb = new NpgsqlConnectionStringBuilder(ConnectionString) { Database = "postgres" };
                    await using var adminConn = new NpgsqlConnection(nb.ConnectionString);
                    await adminConn.OpenAsync();
                    var targetDb = "datafusiondb";
                    var checkSql = $"SELECT 1 FROM pg_database WHERE datname = '{targetDb}'";
                    await using (var checkCmd = new NpgsqlCommand(checkSql, adminConn))
                    {
                        var exists = await checkCmd.ExecuteScalarAsync();
                        if (exists == null)
                        {
                            await using var createCmd = new NpgsqlCommand($@"CREATE DATABASE ""{targetDb}"";", adminConn);
                            await createCmd.ExecuteNonQueryAsync();
                        }
                    }

                    Message = "Cadena aceptada e inicialización completada (PostgreSQL).";
                }
                else // MariaDb
                {
                    // Conectar a servidor y crear base si no existe
                    var mb = new MySqlConnectionStringBuilder(ConnectionString) { Database = "mysql" };
                    await using var adminConn = new MySqlConnection(mb.ConnectionString);
                    await adminConn.OpenAsync();
                    var targetDb = "datafusiondb";
                    var createDbSql = $"CREATE DATABASE IF NOT EXISTS `{targetDb}` CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;";
                    await using (var createCmd = new MySqlCommand(createDbSql, adminConn))
                    {
                        await createCmd.ExecuteNonQueryAsync();
                    }

                    Message = "Cadena aceptada e inicialización completada (MariaDB).";
                }
            }
            catch (System.Exception ex)
            {
                _provider.Clear();
                ModelState.AddModelError(string.Empty, $"No se pudo inicializar la base: {ex.Message}");
                return Page();
            }

            return RedirectToPage("/Index");
        }
    }
}