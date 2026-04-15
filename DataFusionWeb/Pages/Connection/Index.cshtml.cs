using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using System.Threading.Tasks;
using DataFusionArenaWeb.Services;

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

            if (Provider == DatabaseProvider.SqlServer)
            {
                try
                {
                    var repo = new SqlServerRepository(ConnectionString, "DataFusionDb");
                    await repo.InitializeAsync();
                    Message = "Cadena aceptada e inicialización completada (SQL Server).";
                }
                catch (System.Exception ex)
                {
                    _provider.Clear();
                    ModelState.AddModelError(string.Empty, $"No se pudo inicializar la base: {ex.Message}");
                    return Page();
                }
            }
            else if (Provider == DatabaseProvider.Postgres)
            {
                try
                {
                    var repo = new PostgresRepository(ConnectionString, "datafusiondb");
                    var test = await repo.TestConnectionAsync();
                    if (!test.Success) throw new System.Exception(test.Message);
                    await repo.InitializeAsync();
                    Message = "Cadena aceptada e inicialización completada (PostgreSQL).";
                }
                catch (System.Exception ex)
                {
                    _provider.Clear();
                    ModelState.AddModelError(string.Empty, $"No se pudo inicializar PostgreSQL: {ex.Message}");
                    return Page();
                }
            }
            else // MariaDb
            {
                try
                {
                    var repo = new MariaDbRepository(ConnectionString, "datafusiondb");
                    var test = await repo.TestConnectionAsync();
                    if (!test.Success) throw new System.Exception(test.Message);
                    await repo.InitializeAsync();
                    Message = "Cadena aceptada e inicialización completada (MariaDB).";
                }
                catch (System.Exception ex)
                {
                    _provider.Clear();
                    ModelState.AddModelError(string.Empty, $"No se pudo inicializar MariaDB: {ex.Message}");
                    return Page();
                }
            }

            return RedirectToPage("/Index");
        }
    }
}