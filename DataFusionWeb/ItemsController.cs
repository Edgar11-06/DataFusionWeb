using DataFusionArenaWeb.Helpers;
using DataFusionArenaWeb.Models;
using DataFusionArenaWeb.Services;
using Microsoft.AspNetCore.Mvc;
using System.Text;
using System.Text.Json;
using ClosedXML.Excel;
using System.Globalization;
using System.IO;
using System.Threading;

namespace DataFusionArenaWeb.Controllers
{
    public class ItemsController : Controller
    {
        private readonly IInMemoryDataStore _store; // normalmente el router
        private readonly InMemoryDataStore _inMemory; // implementación concreta que contiene los items en memoria
        private readonly ConnectionStringProvider _provider;

        public ItemsController(IInMemoryDataStore store, InMemoryDataStore inMemory, ConnectionStringProvider provider)
        {
            _store = store;
            _inMemory = inMemory;
            _provider = provider;
        }

        // GET /Items
        public IActionResult Index(string categoria = "(Todos)", string sort = "Original")
        {
            var items = _store.Items.ToList();

            // Normalizador: quita diacríticos y caracteres no alfanum, todo en minúsculas.
            

            // Filtrar (usar clave normalizada)
            if (!string.IsNullOrWhiteSpace(categoria) && categoria != "(Todos)")
            {
                var target = NormalizeKey(categoria);
                items = items.Where(i => NormalizeKey(i?.Categoria) == target).ToList();
            }

            // Ordenar
            if (sort?.Contains("Original", StringComparison.OrdinalIgnoreCase) == true)
            {
                if (_store.OriginalOrder != null && _store.OriginalOrder.Count > 0)
                {
                    var index = new Dictionary<DataItem, int>();
                    for (int i = 0; i < _store.OriginalOrder.Count; i++)
                        index[_store.OriginalOrder[i]] = i;

                    items = items.OrderBy(it => index.TryGetValue(it, out var idx) ? idx : int.MaxValue).ToList();
                }
            }
            else
            {
                bool desc = sort?.Contains("Mayor", StringComparison.OrdinalIgnoreCase) == true
                            || sort?.Contains("Desc", StringComparison.OrdinalIgnoreCase) == true;
                items = desc ? items.OrderByDescending(x => x.Valor).ToList() : items.OrderBy(x => x.Valor).ToList();
            }

            // Construir lista de categorías mostrando la etiqueta original pero deduplicando por clave normalizada
            var catSeq = _store.Items.Select(i => (i?.Categoria ?? string.Empty).Trim()).Where(s => !string.IsNullOrEmpty(s));
            var catMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var c in catSeq)
            {
                var key = NormalizeKey(c);
                if (!catMap.ContainsKey(key)) catMap[key] = c;
            }

            ViewBag.Categorias = new List<string> { "(Todos)" }
                .Concat(catMap.Values.OrderBy(s => s, StringComparer.CurrentCultureIgnoreCase))
                .ToList();

            ViewBag.SelectedCategoria = categoria;
            ViewBag.SelectedSort = sort;

            // Información para la vista sobre la conexión SQL
            ViewBag.IsSqlConfigured = _provider.IsSet;
            ViewBag.ConnectionString = _provider.ConnectionString ?? string.Empty;

            // Mensajes de migración / errores
            ViewBag.Message = TempData["Message"] as string;
            ViewBag.Error = TempData["Error"] as string;

            return View(items);
        }

        // POST /Items/Upload
        [HttpPost]
        public async Task<IActionResult> Upload(IFormFile file, CancellationToken ct)
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
        public async Task<IActionResult> MigrateToSql()
        {
            if (!_provider.IsSet || string.IsNullOrWhiteSpace(_provider.ConnectionString))
            {
                TempData["Error"] = "No hay una cadena de conexión configurada. Configure la conexión antes de migrar.";
                return RedirectToAction("Index");
            }

            if (_inMemory.Items == null || !_inMemory.Items.Any())
            {
                TempData["Error"] = "No hay datos en memoria para migrar.";
                return RedirectToAction("Index");
            }

            try
            {
                if (_provider.Provider == DatabaseProvider.Postgres)
                {
                    var repo = new PostgresRepository(_provider.ConnectionString!, "datafusiondb");
                    await repo.InitializeAsync();
                    await repo.SaveItemsAsync(_inMemory.Items);
                }
                else if (_provider.Provider == DatabaseProvider.MariaDb)
                {
                    var repo = new MariaDbRepository(_provider.ConnectionString!, "datafusiondb");
                    await repo.InitializeAsync();
                    await repo.SaveItemsAsync(_inMemory.Items);
                }
                else
                {
                    var repo = new SqlServerRepository(_provider.ConnectionString!, "DataFusionDb");
                    await repo.InitializeAsync();
                    await repo.SaveItemsAsync(_inMemory.Items);
                }

                TempData["Message"] = "Migración completada correctamente.";
            }
            catch (System.Exception ex)
            {
                TempData["Error"] = $"Error al migrar: {ex.Message}";
            }

            return RedirectToAction("Index");
        }

        // GET /Items/Export?format=csv
        public IActionResult Export(string format = "csv", string categoria = "(Todos)")
        {
            var items = _store.Items.AsEnumerable();
            if (!string.IsNullOrWhiteSpace(categoria) && categoria != "(Todos)")
                items = items.Where(i => string.Equals((i?.Categoria ?? string.Empty).Trim(), categoria.Trim(), StringComparison.CurrentCultureIgnoreCase));

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
                ws.Cell(1, 1).Value = "Id";
                ws.Cell(1, 2).Value = "Nombre";
                ws.Cell(1, 3).Value = "Categoria";
                ws.Cell(1, 4).Value = "Cantidad";
                ws.Cell(1, 5).Value = "PrecioUnitario";
                ws.Cell(1, 6).Value = "Valor";
                int r = 2;
                foreach (var it in items)
                {
                    ws.Cell(r, 1).Value = it.Id;
                    ws.Cell(r, 2).Value = it.Nombre;
                    ws.Cell(r, 3).Value = it.Categoria;
                    ws.Cell(r, 4).Value = it.Cantidad;
                    ws.Cell(r, 5).Value = it.PrecioUnitario;
                    ws.Cell(r, 6).Value = it.Valor;
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
                sb.AppendLine("Id,Nombre,Categoria,Cantidad,PrecioUnitario,Valor");
                foreach (var it in items)
                {
                    string Esc(string s) => "\"" + (s ?? string.Empty).Replace("\"", "\"\"") + "\"";
                    sb.AppendLine(string.Join(",",
                        Esc(it.Id),
                        Esc(it.Nombre),
                        Esc(it.Categoria),
                        it.Cantidad.ToString(),
                        it.PrecioUnitario.ToString(CultureInfo.InvariantCulture),
                        it.Valor.ToString(CultureInfo.InvariantCulture)
                    ));
                }
                var bytes = Encoding.UTF8.GetBytes(sb.ToString());
                return File(bytes, "text/csv; charset=utf-8", $"datos_{DateTime.UtcNow:yyyyMMdd_HHmmss}.csv");
            }
        }

        // GET /Items/Group
        public IActionResult Group()
        {
            var groups = _store.Items.GroupBy(i => (i?.Categoria ?? string.Empty).Trim())
                .ToDictionary(g => g.Key, g => g.ToList(), StringComparer.CurrentCultureIgnoreCase);
            return View(groups);
        }

        private static string NormalizeKey(string? s)
        {
            if (string.IsNullOrWhiteSpace(s)) return string.Empty;
            var form = (s ?? string.Empty).Normalize(System.Text.NormalizationForm.FormD);
            var sb = new System.Text.StringBuilder();
            foreach (var ch in form)
            {
                var uc = System.Globalization.CharUnicodeInfo.GetUnicodeCategory(ch);
                if (uc != System.Globalization.UnicodeCategory.NonSpacingMark)
                    sb.Append(ch);
            }
            var cleaned = sb.ToString().Normalize(System.Text.NormalizationForm.FormC).ToLowerInvariant();
            cleaned = System.Text.RegularExpressions.Regex.Replace(cleaned, @"[^\p{L}\p{Nd}]+", string.Empty);
            return cleaned.Trim();
        }
    }
}