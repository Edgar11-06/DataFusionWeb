using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Xml.Linq;
using System.Linq;
using DataFusionArenaWeb.Models;

namespace DataFusionArenaWeb.Helpers
{
    // Parser ligero que replica la lógica del Form1/TxtToDataItems/JsonToDataItems
    public static class FileParser
    {
        // Lee y parsea un IFormFile a List<DataItem>
        public static async Task<List<DataItem>> ParseFormFileAsync(IFormFile file, CancellationToken ct = default)
        {
            if (file == null) return new List<DataItem>();

            // Leer todo a memoria (fácil para archivos pequeños/medianos)
            using var ms = new MemoryStream();
            await file.CopyToAsync(ms, ct);
            ms.Position = 0;

            var ext = Path.GetExtension(file.FileName).ToLowerInvariant();
            if (ext == ".json")
            {
                return ParseJson(ms);
            }
            else if (ext == ".xml")
            {
                try
                {
                    ms.Position = 0;
                    var doc = XDocument.Load(ms);
                    var items = new List<DataItem>();

                    // función auxiliar para normalizar nombre de etiqueta/atributo (usa la normalización ya definida)
                    string Norm(string s) => string.IsNullOrWhiteSpace(s) ? string.Empty : NormalizeHeader(s);

                    // sinónimos normalizados
                    var idAliases = new[] { "id", "id_venta", "idventa", "venta", "codigo", "identifier" }.Select(Norm).ToHashSet();
                    var nombreAliases = new[] { "nombre", "nombre_producto", "producto", "name", "title" }.Select(Norm).ToHashSet();
                    var categoriaAliases = new[] { "categoria", "categoria_producto", "category", "grupo", "tipo" }.Select(Norm).ToHashSet();
                    var cantidadAliases = new[] { "cantidad", "qty", "quantity", "unidades" }.Select(Norm).ToHashSet();
                    var precioAliases = new[] { "preciounitario", "precio_unitario", "precio", "unit_price", "precio_unitario" }.Select(Norm).ToHashSet();
                    var valorAliases = new[] { "valor", "total", "total_venta", "amount" }.Select(Norm).ToHashSet();

                    // elegir nodos candidatos: elementos que tienen hijos (posibles registros)
                    // var candidateNodes = doc.Descendants()
                    //     .Where(e => e.Elements().Any())
                    //     .ToList();
                    var candidateNodes = doc.Root?.Elements().Where(e => e.HasElements).ToList() ?? new List<XElement>();

                    // si no hay nodos con hijos, tomar hijos directos del root
                    if (!candidateNodes.Any())
                        candidateNodes = doc.Root?.Elements().ToList() ?? new List<XElement>();

                    foreach (var el in candidateNodes)
                    {
                        string GetValFromElementOrAttr(params string[] aliases)
                        {
                            var aliasNorms = aliases.Select(Norm).ToArray();

                            // buscar por hijo
                            foreach (var ch in el.Elements())
                            {
                                var n = Norm(ch.Name.LocalName);
                                if (aliasNorms.Contains(n)) return (ch.Value ?? string.Empty).Trim();
                            }

                            // buscar por atributo
                            foreach (var at in el.Attributes())
                            {
                                var n = Norm(at.Name.LocalName);
                                if (aliasNorms.Contains(n)) return (at.Value ?? string.Empty).Trim();
                            }

                            // buscar por coincidencia aproximada en los hijos (p.e. tags con underscores distintos)
                            foreach (var ch in el.Elements())
                            {
                                var n = Norm(ch.Name.LocalName);
                                if (aliasNorms.Any(a => n.Contains(a) || a.Contains(n))) return (ch.Value ?? string.Empty).Trim();
                            }

                            return string.Empty;
                        }

                        var di = new DataItem
                        {
                            Id = GetValFromElementOrAttr("Id", "ID_Venta", "IDVenta", "Venta", "codigo", "identifier"),
                            Nombre = GetValFromElementOrAttr("Nombre", "Nombre_Producto", "producto", "name"),
                            Categoria = GetValFromElementOrAttr("Categoria", "Categoria_Producto", "category"),
                            Cantidad = int.TryParse(GetValFromElementOrAttr("Cantidad", "cantidad", "qty"), NumberStyles.Integer, CultureInfo.InvariantCulture, out var c) ? c : 0,
                            PrecioUnitario = decimal.TryParse(GetValFromElementOrAttr("Precio_Unitario", "PrecioUnitario", "precio", "unit_price"), NumberStyles.Number, CultureInfo.InvariantCulture, out var p) ? p : 0m,
                            Valor = decimal.TryParse(GetValFromElementOrAttr("Total_Venta", "Valor", "total", "amount"), NumberStyles.Number, CultureInfo.InvariantCulture, out var v) ? v : 0m
                        };

                        if (!string.IsNullOrWhiteSpace(di.Id)) items.Add(di);
                    }

                    return items;
                }
                catch
                {
                    return new List<DataItem>();
                }
            }
            else
            {
                ms.Position = 0;
                using var sr = new StreamReader(ms, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, leaveOpen: true);
                var lines = new List<string>();
                while (!sr.EndOfStream)
                {
                    var l = await sr.ReadLineAsync();
                    if (!string.IsNullOrWhiteSpace(l)) lines.Add(l);
                }

                if (lines.Count == 0) return new List<DataItem>();

                var first = lines.First();
                var sep = DetectSeparatorFromLine(first);
                var hasHeader = DetectHeaderInLine(first, sep);

                return ParseLines(lines, sep, hasHeader);
            }
        }

        private static List<DataItem> ParseJson(Stream ms)
        {
            try
            {
                ms.Position = 0;
                using var doc = JsonDocument.Parse(ms);
                var root = doc.RootElement;
                var items = new List<DataItem>();

                // Si la raíz es un array, procesarlo directamente
                if (root.ValueKind == JsonValueKind.Array)
                {
                    foreach (var el in root.EnumerateArray())
                    {
                        var maybe = ElementToDataItem(el);
                        if (maybe != null) items.Add(maybe);
                    }
                    return items;
                }

                // Si la raíz es un objeto, buscar arrays anidados (p.e. { "data": [...] } o { "items": [...] })
                if (root.ValueKind == JsonValueKind.Object)
                {
                    // Primero, propiedades que sean arrays
                    foreach (var prop in root.EnumerateObject())
                    {
                        if (prop.Value.ValueKind == JsonValueKind.Array)
                        {
                            foreach (var el in prop.Value.EnumerateArray())
                            {
                                var maybe = ElementToDataItem(el);
                                if (maybe != null) items.Add(maybe);
                            }
                            if (items.Any()) return items;
                        }
                    }

                    // Si no se encontró array, intentar propiedades comunes que contengan arrays en profundidad
                    var candidates = new[] { "items", "data", "rows", "result", "results" };
                    foreach (var name in candidates)
                    {
                        if (root.TryGetProperty(name, out var prop) && prop.ValueKind == JsonValueKind.Array)
                        {
                            foreach (var el in prop.EnumerateArray())
                            {
                                var maybe = ElementToDataItem(el);
                                if (maybe != null) items.Add(maybe);
                            }
                            if (items.Any()) return items;
                        }
                    }

                    // Finalmente intentar mapear la raíz como un único objeto (si contiene Id)
                    var maybeSingle = ElementToDataItem(root);
                    if (maybeSingle != null) items.Add(maybeSingle);
                    return items;
                }

                return new List<DataItem>();
            }
            catch
            {
                // intentar NDJSON (una línea JSON por objeto)
                ms.Position = 0;
                using var sr = new StreamReader(ms, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, leaveOpen: true);
                var result = new List<DataItem>();
                string? line;
                while ((line = sr.ReadLine()) != null)
                {
                    if (string.IsNullOrWhiteSpace(line)) continue;
                    try
                    {
                        using var doc = JsonDocument.Parse(line);
                        var el = doc.RootElement;
                        var maybe = ElementToDataItem(el);
                        if (maybe != null) result.Add(maybe);
                    }
                    catch { continue; }
                }
                return result;
            }
        }

        private static DataItem? ElementToDataItem(JsonElement el)
        {
            try
            {
                if (el.ValueKind != JsonValueKind.Object) return null;

                // Construir diccionario: clave = nombre normalizado, valor = JsonElement
                var map = new Dictionary<string, JsonElement>(StringComparer.OrdinalIgnoreCase);
                foreach (var prop in el.EnumerateObject())
                {
                    var norm = NormalizeHeader(prop.Name);
                    if (!map.ContainsKey(norm)) map[norm] = prop.Value;
                }

                string GetStringFromMap(params string[] names)
                {
                    foreach (var n in names)
                    {
                        var key = NormalizeHeader(n);
                        if (map.TryGetValue(key, out var v)) return v.ToString() ?? string.Empty;
                    }
                    // fallback: buscar coincidencias parciales
                    foreach (var kv in map)
                    {
                        foreach (var n in names)
                        {
                            var key = NormalizeHeader(n);
                            if (kv.Key.Contains(key) || key.Contains(kv.Key)) return kv.Value.ToString() ?? string.Empty;
                        }
                    }
                    return string.Empty;
                }

                string id = GetStringFromMap("Id", "ID_Venta", "id_venta", "idventa", "venta", "codigo", "identifier");
                if (string.IsNullOrWhiteSpace(id)) return null;

                var nombre = GetStringFromMap("Nombre", "Nombre_Producto", "nombre_producto", "producto", "name");
                var categoria = GetStringFromMap("Categoria", "Categoria_Producto", "categoria_producto", "category");
                var cantidadStr = GetStringFromMap("Cantidad", "cantidad", "qty");
                var precioUnitStr = GetStringFromMap("Precio_Unitario", "PrecioUnitario", "precio_unitario", "precio", "unit_price");
                var valorStr = GetStringFromMap("Total_Venta", "TotalVenta", "total_venta", "valor", "amount");

                int.TryParse(cantidadStr, NumberStyles.Integer, CultureInfo.InvariantCulture, out var cantidad);
                TryParseDecimal(precioUnitStr, out var precioUnitario);
                TryParseDecimal(valorStr, out var valor);

                return new DataItem
                {
                    Id = id.Trim().Trim('\"'),
                    Nombre = nombre.Trim(),
                    Categoria = categoria.Trim(),
                    Cantidad = cantidad,
                    PrecioUnitario = precioUnitario,
                    Valor = valor
                };
            }
            catch
            {
                return null;
            }
        }

        // Parse lines de CSV/TXT usando heurísticas (basado en tu TxtToDataItems)
        private static List<DataItem> ParseLines(List<string> lines, char separator, bool hasHeader)
        {
            var items = new List<DataItem>();
            int start = 0;
            Dictionary<string, int>? mapping = null;
            List<string>? headerFields = null;

            if (hasHeader)
            {
                headerFields = ParseLine(lines[0], separator).Select(h => h.Trim()).ToList();
                mapping = AutoMapHeaders(headerFields);
                start = 1;
            }

            if (mapping == null)
            {
                var sample = ParseLine(lines[start], separator);
                mapping = HeuristicPositionalMap(sample.Count);
            }

            for (int i = start; i < lines.Count; i++)
            {
                var parts = ParseLine(lines[i], separator).ToArray();
                if (!LineHasRequiredColumns(parts, mapping!))
                {
                    // intentar reparsing con otros separadores
                    var altSeparators = new[] { ',', ';', '\t', '|', ':' }.Distinct().ToArray();
                    bool ok = false;
                    foreach (var alt in altSeparators)
                    {
                        if (alt == separator) continue;
                        var altParts = ParseLine(lines[i], alt).ToArray();
                        if (LineHasRequiredColumns(altParts, mapping!))
                        {
                            parts = altParts;
                            ok = true;
                            break;
                        }
                    }
                    if (!ok && !AttemptFillByIdRegex(ref parts, lines[i], mapping!)) continue;
                }

                string Safe(int idx) => (idx >= 0 && idx < parts.Length) ? (parts[idx] ?? string.Empty).Trim() : string.Empty;
                var idRaw = Safe(mapping!["Id"]);
                var id = ExtractIdFromField(idRaw, lines[i]);
                if (string.IsNullOrEmpty(id)) continue;

                var nombre = mapping!["Nombre"] >= 0 ? Safe(mapping!["Nombre"]) : string.Empty;
                var categoria = mapping!["Categoria"] >= 0 ? Safe(mapping!["Categoria"]) : string.Empty;
                var cantidadStr = mapping!.ContainsKey("Cantidad") && mapping!["Cantidad"] >= 0 ? Safe(mapping!["Cantidad"]) : string.Empty;
                var precioUnitStr = mapping!.ContainsKey("PrecioUnitario") && mapping!["PrecioUnitario"] >= 0 ? Safe(mapping!["PrecioUnitario"]) : string.Empty;
                var valorStr = Safe(mapping!["Valor"]);

                if (!int.TryParse(cantidadStr, NumberStyles.Integer, CultureInfo.InvariantCulture, out int cantidad))
                    int.TryParse(cantidadStr, NumberStyles.Integer, CultureInfo.CurrentCulture, out cantidad);

                if (!TryParseDecimal(precioUnitStr, out decimal precioUnitario))
                    precioUnitario = 0m;

                if (!TryParseDecimal(valorStr, out decimal valor))
                    valor = 0m;

                items.Add(new DataItem
                {
                    Id = id,
                    Nombre = nombre,
                    Categoria = categoria,
                    Cantidad = cantidad,
                    PrecioUnitario = precioUnitario,
                    Valor = valor
                });
            }

            return items;
        }

        // UTILIDADES (adaptadas)

        public static char DetectSeparatorFromLine(string line)
        {
            var candidates = new[] { ',', ';', '\t', '|', ':' };
            char best = ',';
            int bestCount = -1;
            foreach (var c in candidates)
            {
                int count = line.Count(ch => ch == c);
                if (count > bestCount)
                {
                    bestCount = count;
                    best = c;
                }
            }
            return best;
        }

        public static bool DetectHeaderInLine(string line, char separator)
        {
            var parts = line.Split(separator);
            if (parts.Length <= 1) return false;

            bool anyLetters = parts.Any(p => Regex.IsMatch(p ?? string.Empty, @"[A-Za-zÁÉÍÓÚáéíóúÑñ]"));
            bool allNumeric = parts.All(p =>
            {
                if (string.IsNullOrWhiteSpace(p)) return false;
                var s = p.Trim();
                return decimal.TryParse(s, NumberStyles.Number, CultureInfo.InvariantCulture, out _)
                    || decimal.TryParse(s, NumberStyles.Number, CultureInfo.CurrentCulture, out _);
            });

            return anyLetters && !allNumeric;
        }

        private static List<string> ParseLine(string line, char separator)
        {
            var result = new List<string>();
            if (string.IsNullOrEmpty(line)) return result;

            if (line.Length >= 2 && line[0] == '\"' && line[line.Length - 1] == '\"')
            {
                var inner = line.Substring(1, line.Length - 2).Replace("\"\"", "\"");
                line = inner;
            }

            var sb = new StringBuilder();
            bool inQuotes = false;
            for (int i = 0; i < line.Length; i++)
            {
                char c = line[i];
                if (c == '\"')
                {
                    if (inQuotes && i + 1 < line.Length && line[i + 1] == '\"')
                    {
                        sb.Append('\"'); i++;
                    }
                    else inQuotes = !inQuotes;
                }
                else if (c == separator && !inQuotes)
                {
                    result.Add(sb.ToString());
                    sb.Clear();
                }
                else sb.Append(c);
            }
            result.Add(sb.ToString());
            return result;
        }

        private static Dictionary<string, int>? AutoMapHeaders(IList<string> headers)
        {
            var norm = headers.Select((h, i) => new { Orig = h, Norm = NormalizeHeader(h), Index = i }).ToList();

            int? find(params string[] candidates)
            {
                foreach (var c in candidates)
                {
                    var exact = norm.FirstOrDefault(x => x.Norm == c);
                    if (exact != null) return exact.Index;
                }
                foreach (var c in candidates)
                {
                    var contains = norm.FirstOrDefault(x => x.Norm.Contains(c));
                    if (contains != null) return contains.Index;
                }
                return null;
            }

            int? idIdx = find("id", "id_venta", "venta", "identifier", "codigo");
            int? nombreIdx = find("nombre", "nombre_producto", "producto", "title", "name");
            int? categoriaIdx = find("categoria", "categoria_producto", "category", "grupo", "tipo");
            int? cantidadIdx = find("cantidad", "qty", "quantity", "unidades");
            int? precioUnitIdx = find("precio_unitario", "preciounitario", "precio_unit", "unit_price", "precio");
            int? valorIdx = find("total", "total_venta", "precio_unitario", "precio", "valor", "amount");

            if (!idIdx.HasValue && norm.Count > 0) idIdx = 0;
            if (!valorIdx.HasValue && norm.Count > 0) valorIdx = norm.Count - 1;
            if (!idIdx.HasValue || !valorIdx.HasValue) return null;

            return new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
            {
                ["Id"] = idIdx.Value,
                ["Nombre"] = nombreIdx ?? -1,
                ["Categoria"] = categoriaIdx ?? -1,
                ["Cantidad"] = cantidadIdx ?? -1,
                ["PrecioUnitario"] = precioUnitIdx ?? -1,
                ["Valor"] = valorIdx.Value
            };
        }

        private static Dictionary<string, int> HeuristicPositionalMap(int columnCount)
        {
            return new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
            {
                ["Id"] = columnCount > 0 ? 0 : -1,
                ["Nombre"] = columnCount > 1 ? 1 : -1,
                ["Categoria"] = columnCount > 2 ? 2 : -1,
                ["Cantidad"] = columnCount > 3 ? 3 : -1,
                ["PrecioUnitario"] = columnCount > 4 ? columnCount - 2 : -1,
                ["Valor"] = columnCount > 0 ? columnCount - 1 : -1
            };
        }

        private static bool LineHasRequiredColumns(string[] parts, Dictionary<string,int> mapping)
        {
            if (parts == null) return false;
            if (!mapping.TryGetValue("Id", out var idIdx) || !mapping.TryGetValue("Valor", out var valIdx))
                return false;
            return idIdx >= 0 && idIdx < parts.Length && valIdx >= 0 && valIdx < parts.Length;
        }

        private static bool AttemptFillByIdRegex(ref string[] parts, string entireLine, Dictionary<string,int> mapping)
        {
            var m = Regex.Match(entireLine, @"\b[A-Za-z]{1,}[0-9]{1,}\b");
            if (!m.Success) return false;

            var idVal = m.Value;
            if (mapping["Id"] == 0)
            {
                int size = Math.Max(mapping.Values.Max() + 1, 1);
                var newParts = new string[size];
                for (int j = 0; j < size; j++) newParts[j] = string.Empty;
                newParts[0] = idVal;
                parts = newParts;
                return true;
            }
            return false;
        }

        private static string ExtractIdFromField(string fieldValue, string entireLine)
        {
            if (string.IsNullOrWhiteSpace(fieldValue)) fieldValue = entireLine ?? string.Empty;
            var cleaned = fieldValue.Trim().Trim('\"');
            var m = Regex.Match(cleaned, @"\b[A-Za-z]{1,}[0-9]{1,}\b");
            if (m.Success) return m.Value;
            int idx = cleaned.IndexOfAny(new[] { ',', ';', '|', '\t' });
            if (idx > 0) return cleaned.Substring(0, idx).Trim().Trim('\"');
            return cleaned;
        }

        private static bool TryParseDecimal(string s, out decimal value)
        {
            value = 0m;
            if (string.IsNullOrWhiteSpace(s)) return false;
            if (decimal.TryParse(s, NumberStyles.Number, CultureInfo.InvariantCulture, out value)) return true;
            if (decimal.TryParse(s, NumberStyles.Number, CultureInfo.CurrentCulture, out value)) return true;
            var cleaned = Regex.Replace(s, @"[^\d\-\+\,\.]", string.Empty);
            if (decimal.TryParse(cleaned, NumberStyles.Number, CultureInfo.InvariantCulture, out value)) return true;
            if (decimal.TryParse(cleaned, NumberStyles.Number, CultureInfo.CurrentCulture, out value)) return true;
            return false;
        }

        private static string NormalizeHeader(string s)
        {
            if (string.IsNullOrEmpty(s)) return string.Empty;
            var formD = s.Normalize(System.Text.NormalizationForm.FormD);
            var sb = new StringBuilder();
            foreach (var ch in formD)
            {
                var uc = CharUnicodeInfo.GetUnicodeCategory(ch);
                if (uc != System.Globalization.UnicodeCategory.NonSpacingMark)
                    sb.Append(ch);
            }
            var cleaned = sb.ToString().Normalize(System.Text.NormalizationForm.FormC).ToLowerInvariant();
            cleaned = Regex.Replace(cleaned, @"[^\p{L}\p{Nd}]+", "_");
            cleaned = Regex.Replace(cleaned, @"_+", "_").Trim('_');
            return cleaned;
        }
    }
}