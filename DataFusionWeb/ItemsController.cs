using DataFusionArenaWeb.Helpers;
using DataFusionArenaWeb.Services;
using Microsoft.AspNetCore.Mvc;
using System.Text;
using System.Text.Json;
using ClosedXML.Excel;
using System.Globalization;

namespace DataFusionArenaWeb.Controllers
{
    public class ItemsController : Controller
    {
        private readonly IInMemoryDataStore _store;
        private readonly ConnectionStringProvider _provider;

        public ItemsController(IInMemoryDataStore store, ConnectionStringProvider provider)
        {
            _store = store;
            _provider = provider;
        }

        // GET /Items
        public IActionResult Index(string categoria = "(Todos)", string sort = "Original", string? filterColumn = null, string? filterValue = null)
        {
            var items = _store.Items.ToList();

            // Todas las columnas presentes (para el selector de filtro)
            var allKeys = items.SelectMany(d => d.Keys).Distinct(StringComparer.OrdinalIgnoreCase).ToList();
            ViewBag.FilterColumns = allKeys;
            ViewBag.SelectedFilterColumn = filterColumn ?? string.Empty;
            ViewBag.FilterValue = filterValue ?? string.Empty;

            // Construir lista de categorías dinámicamente si existe columna tipo categoría
            var catCandidates = new[] { "categoria", "category", "grupo", "tipo", "categoria_producto" };
            string? categoryKey = DetectKey(items, catCandidates);

            var catSeq = items.Select(r => GetStringValue(r, categoryKey)).Where(s => !string.IsNullOrWhiteSpace(s)).Distinct(StringComparer.CurrentCultureIgnoreCase).ToList();
            var catList = new List<string> { "(Todos)" }.Concat(catSeq.OrderBy(s => s, StringComparer.CurrentCultureIgnoreCase)).ToList();
            ViewBag.Categorias = catList;
            ViewBag.SelectedCategoria = categoria;
            ViewBag.SelectedSort = sort;
            ViewBag.IsSqlConfigured = _provider.IsSet;
            ViewBag.ConnectionString = _provider.ConnectionString ?? string.Empty;
            ViewBag.Message = TempData["Message"] as string;
            ViewBag.Error = TempData["Error"] as string;

            // Filtrar por categoría si se detectó columna
            if (!string.IsNullOrWhiteSpace(categoryKey) && !string.IsNullOrWhiteSpace(categoria) && categoria != "(Todos)")
            {
                items = items.Where(r => string.Equals(GetStringValue(r, categoryKey), categoria, StringComparison.CurrentCultureIgnoreCase)).ToList();
            }

            // Filtrado dinámico por columna:
            // - si se selecciona columna pero no se ingresa valor -> mostrar sólo filas donde la columna tenga valor (no nulo/empty)
            // - si se selecciona columna y se ingresa valor -> buscar contains (case-insensitive)
            if (!string.IsNullOrWhiteSpace(filterColumn))
            {
                if (string.IsNullOrWhiteSpace(filterValue))
                {
                    items = items.Where(r =>
                    {
                        var s = GetStringValue(r, filterColumn);
                        return !string.IsNullOrWhiteSpace(s);
                    }).ToList();
                }
                else
                {
                    items = items.Where(r =>
                    {
                        var s = GetStringValue(r, filterColumn);
                        return !string.IsNullOrWhiteSpace(s) && s.IndexOf(filterValue!, StringComparison.CurrentCultureIgnoreCase) >= 0;
                    }).ToList();
                }
            }

            // Orden simple: si piden ordenar por Valor detectamos columna numérica
            if (!string.IsNullOrWhiteSpace(sort) && !sort.Contains("Original", StringComparison.OrdinalIgnoreCase))
            {
                bool desc = sort.Contains("Mayor", StringComparison.OrdinalIgnoreCase) || sort.Contains("Desc", StringComparison.OrdinalIgnoreCase);
                var valueKey = DetectKey(items, new[] { "valor", "total", "amount", "precio", "precio_unitario" });
                if (!string.IsNullOrWhiteSpace(valueKey))
                {
                    items = desc ? items.OrderByDescending(r => GetDecimalValue(r, valueKey)).ToList()
                                 : items.OrderBy(r => GetDecimalValue(r, valueKey)).ToList();
                }
            }

            return View(items);
        }

        // GET /Items/Charts
        [HttpGet]
        public IActionResult Charts()
        {
            ViewBag.IsSqlConfigured = _provider.IsSet;
            ViewBag.ConnectionString = _provider.ConnectionString ?? string.Empty;
            return View();
        }

        private record CategoryStat(string Categoria, int Cantidad, decimal Valor);

        // GET /Items/CategoryStats
        [HttpGet]
        public IActionResult CategoryStats()
        {
            var items = _store.Items;
            if (items == null || !items.Any())
                return Json(new[] { new CategoryStat("(Sin datos)", 0, 0m) });

            var allKeys = items.SelectMany(d => d.Keys).Distinct(StringComparer.OrdinalIgnoreCase).ToList();

            var categoryKey = DetectKey(items, new[] { "categoria", "category", "grupo", "tipo", "categoria_producto" })
                              ?? allKeys.FirstOrDefault();

            var valueKey = DetectKey(items, new[] { "valor", "total", "amount", "precio", "precio_unitario" });

            var groups = items
                .GroupBy(r => GetStringValue(r, categoryKey) ?? "(Sin valor)", StringComparer.CurrentCultureIgnoreCase)
                .Select(g => new CategoryStat(
                    g.Key,
                    g.Count(),
                    valueKey == null ? 0m : g.Sum(r => GetDecimalValue(r, valueKey))
                ))
                .OrderByDescending(x => x.Cantidad)
                .ToList();

            // Si hay demasiadas categorías, quedarnos con top N y agregar "Otros"
            const int maxCategories = 10;
            if (groups.Count > maxCategories)
            {
                var top = groups.Take(maxCategories).ToList();
                var others = groups.Skip(maxCategories).ToList();
                var othersAgg = new CategoryStat(
                    "Otros",
                    others.Sum(x => x.Cantidad),
                    others.Sum(x => x.Valor)
                );
                top.Add(othersAgg);
                groups = top;
            }

            // Aseguramos al menos un elemento (por si algo raro sucediera)
            if (!groups.Any())
                groups = new List<CategoryStat> { new CategoryStat("(Sin datos)", 0, 0m) };

            return Json(groups);
        }

        // POST /Items/Upload
        [HttpPost]
        public async Task<IActionResult> Upload(Microsoft.AspNetCore.Http.IFormFile file, CancellationToken ct)
        {
            if (file == null || file.Length == 0)
            {
                TempData["Error"] = "No se subió ningún archivo.";
                return RedirectToAction("Index");
            }

            var parsed = await FileParser.ParseFormFileAsync(file, ct);
            _store.SetItems(parsed);

            TempData["Message"] = $"Archivo procesado. Elementos parseados: {parsed?.Count ?? 0}";
            return RedirectToAction("Index");
        }

        // POST /Items/MigrateToSql
        [HttpPost]
        public async Task<IActionResult> MigrateToSql(string? tableName)
        {
            // Verificamos que haya cadena configurada
            if (!_provider.IsSet)
            {
                TempData["Error"] = "No hay cadena de conexión configurada. Ve a 'Configurar conexión' y define la conexión.";
                return RedirectToAction("Index");
            }

            // Si no hay datos no hacemos nada
            var rows = _store.Items;
            if (rows == null || !rows.Any())
            {
                TempData["Error"] = "No hay datos cargados para migrar.";
                return RedirectToAction("Index");
            }

            try
            {
                int inserted = 0;
                // si el usuario pasa un nombre vacío lo dejamos null para que el migrator genere uno
                var rawTableName = string.IsNullOrWhiteSpace(tableName) ? null : tableName.Trim();

                switch (_provider.Provider)
                {
                    case DatabaseProvider.SqlServer:
                        var sqlMigrator = HttpContext.RequestServices.GetService(typeof(SqlServerDataMigrator)) as SqlServerDataMigrator;
                        if (sqlMigrator == null) { TempData["Error"] = "Servicio de migración a SQL Server no disponible."; return RedirectToAction("Index"); }
                        inserted = await sqlMigrator.MigrateAsync(rows, rawTableName, CancellationToken.None);
                        break;

                    case DatabaseProvider.MariaDb:
                        var mariaMigrator = HttpContext.RequestServices.GetService(typeof(MariaDbDataMigrator)) as MariaDbDataMigrator;
                        if (mariaMigrator == null) { TempData["Error"] = "Servicio de migración a MariaDB no disponible."; return RedirectToAction("Index"); }
                        inserted = await mariaMigrator.MigrateAsync(rows, rawTableName, CancellationToken.None);
                        break;

                    case DatabaseProvider.Postgres:
                        var pgMigrator = HttpContext.RequestServices.GetService(typeof(PostgresDataMigrator)) as PostgresDataMigrator;
                        if (pgMigrator == null) { TempData["Error"] = "Servicio de migración a PostgreSQL no disponible."; return RedirectToAction("Index"); }
                        inserted = await pgMigrator.MigrateAsync(rows, rawTableName, CancellationToken.None);
                        break;

                    default:
                        TempData["Error"] = "Proveedor de base de datos no soportado para migración.";
                        return RedirectToAction("Index");
                }

                TempData["Message"] = $"Migración completada. Filas insertadas: {inserted}.";
            }
            catch (Exception ex)
            {
                TempData["Error"] = $"Error migrando: {ex.Message}";
            }

            return RedirectToAction("Index");
        }

        // GET /Items/Export?format=csv
        public IActionResult Export(string format = "csv")
        {
            var items = _store.Items.AsEnumerable();
            if (!items.Any()) return RedirectToAction("Index");

            format = format?.ToLowerInvariant();
            if (format == "json")
            {
                var bytes = JsonSerializer.SerializeToUtf8Bytes(items, new JsonSerializerOptions { WriteIndented = true });
                return File(bytes, "application/json", $"datos_{DateTime.UtcNow:yyyyMMdd_HHmmss}.json");
            }
            else if (format == "xlsx" || format == "excel")
            {
                using var wb = new XLWorkbook();
                var ws = wb.AddWorksheet("Datos");

                // columnas: union de todas las claves en orden consistente
                var allKeys = items.SelectMany(d => d.Keys).Distinct(StringComparer.OrdinalIgnoreCase).ToList();

                for (int c = 0; c < allKeys.Count; c++)
                    ws.Cell(1, c + 1).Value = allKeys[c];

                int r = 2;
                foreach (var row in items)
                {
                    for (int c = 0; c < allKeys.Count; c++)
                    {
                        var k = allKeys[c];
                        if (row.TryGetValue(k, out var v) && v != null)
                            ws.Cell(r, c + 1).SetValue(v.ToString() ?? string.Empty);
                        else
                            ws.Cell(r, c + 1).SetValue(string.Empty);
                    }
                    r++;
                }

                ws.Columns().AdjustToContents();
                using var ms = new MemoryStream();
                wb.SaveAs(ms);
                return File(ms.ToArray(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", $"datos_{DateTime.UtcNow:yyyyMMdd_HHmmss}.xlsx");
            }
            else
            {
                // CSV
                var sb = new StringBuilder();
                var allKeys = items.SelectMany(d => d.Keys).Distinct(StringComparer.OrdinalIgnoreCase).ToList();
                string Esc(object? o)
                {
                    var s = o?.ToString() ?? string.Empty;
                    if (s.Contains('"') || s.Contains(',') || s.Contains('\n'))
                        return "\"" + s.Replace("\"", "\"\"") + "\"";
                    return s;
                }

                sb.AppendLine(string.Join(",", allKeys));
                foreach (var row in items)
                {
                    var vals = allKeys.Select(k => row.TryGetValue(k, out var v) ? Esc(v) : string.Empty);
                    sb.AppendLine(string.Join(",", vals));
                }

                var bytes = Encoding.UTF8.GetBytes(sb.ToString());
                return File(bytes, "text/csv; charset=utf-8", $"datos_{DateTime.UtcNow:yyyyMMdd_HHmmss}.csv");
            }
        }

        // GET /Items/Group
        public IActionResult Group()
        {
            var items = _store.Items;
            var categoryKey = DetectKey(items, new[] { "categoria", "category", "grupo", "tipo", "categoria_producto" });
            if (categoryKey == null) return View(new Dictionary<string, List<Dictionary<string, object?>>>()); 

            var groups = items.GroupBy(i => GetStringValue(i, categoryKey) ?? string.Empty, StringComparer.CurrentCultureIgnoreCase)
                .ToDictionary(g => g.Key, g => g.ToList(), StringComparer.CurrentCultureIgnoreCase);

            return View(groups);
        }

        // Helpers
        private static string? DetectKey(IEnumerable<Dictionary<string, object?>> items, IEnumerable<string> candidates)
        {
            var set = new HashSet<string>(items.SelectMany(d => d.Keys), StringComparer.OrdinalIgnoreCase);
            foreach (var c in candidates)
            {
                var found = set.FirstOrDefault(k => string.Equals(k, c, StringComparison.OrdinalIgnoreCase));
                if (found != null) return found;
            }
            // fallback: try partial contains
            foreach (var c in candidates)
            {
                var found = set.FirstOrDefault(k => k.IndexOf(c, StringComparison.OrdinalIgnoreCase) >= 0);
                if (found != null) return found;
            }
            return null;
        }

        private static string? GetStringValue(Dictionary<string, object?> row, string? key)
        {
            if (string.IsNullOrWhiteSpace(key) || row == null) return null;
            if (row.TryGetValue(key, out var v) && v != null) return v.ToString();
            // try case-insensitive
            var kv = row.FirstOrDefault(k => string.Equals(k.Key, key, StringComparison.OrdinalIgnoreCase));
            return kv.Equals(default(KeyValuePair<string, object?>)) ? null : kv.Value?.ToString();
        }

        private static decimal GetDecimalValue(Dictionary<string, object?> row, string? key)
        {
            if (string.IsNullOrWhiteSpace(key) || row == null) return 0m;
            if (row.TryGetValue(key, out var v) && v != null)
            {
                if (v is decimal d) return d;
                if (v is double db) return Convert.ToDecimal(db);
                if (v is float f) return Convert.ToDecimal(f);
                if (v is long l) return l;
                if (v is int i) return i;
                if (decimal.TryParse(v.ToString(), NumberStyles.Number | NumberStyles.AllowCurrencySymbol, CultureInfo.InvariantCulture, out var parsed)) return parsed;
                if (decimal.TryParse(v.ToString(), NumberStyles.Number | NumberStyles.AllowCurrencySymbol, CultureInfo.CurrentCulture, out parsed)) return parsed;
            }

            // fallback: search keys case-insensitive
            var kv = row.FirstOrDefault(k => string.Equals(k.Key, key, StringComparison.OrdinalIgnoreCase));
            if (!kv.Equals(default(KeyValuePair<string, object?>)) && kv.Value != null)
            {
                var s = kv.Value.ToString();
                if (decimal.TryParse(s, NumberStyles.Number | NumberStyles.AllowCurrencySymbol, CultureInfo.InvariantCulture, out var parsed2)) return parsed2;
                if (decimal.TryParse(s, NumberStyles.Number | NumberStyles.AllowCurrencySymbol, CultureInfo.CurrentCulture, out parsed2)) return parsed2;
            }

            return 0m;
        }
    }
}