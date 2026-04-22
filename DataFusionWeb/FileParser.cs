using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Xml.Linq;

namespace DataFusionArenaWeb.Helpers
{
    // Parser que devuelve List<Dictionary<string, object?>>
    public static class FileParser
    {
        public static async Task<List<Dictionary<string, object?>>> ParseFormFileAsync(Microsoft.AspNetCore.Http.IFormFile file, CancellationToken ct = default)
        {
            var result = new List<Dictionary<string, object?>>();
            if (file == null) return result;

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
                    return ParseXml(doc);
                }
                catch
                {
                    return result;
                }
            }
            else
            {
                // TXT/CSV-like
                ms.Position = 0;
                using var sr = new StreamReader(ms, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, leaveOpen: true);
                var text = await sr.ReadToEndAsync();
                if (string.IsNullOrWhiteSpace(text)) return result;

                // Split into logical records respecting quoted newlines
                var lines = SplitRecords(text);
                if (lines.Count == 0) return result;

                var first = lines.First();
                var sep = DetectSeparatorFromLine(first);
                var hasHeader = DetectHeaderInLine(first, sep);

                return ParseLines(lines, sep, hasHeader);
            }
        }

        private static List<string> SplitRecords(string text)
        {
            var records = new List<string>();
            if (text == null) return records;

            var sb = new StringBuilder();
            bool inQuotes = false;
            for (int i = 0; i < text.Length; i++)
            {
                char c = text[i];

                if (c == '"')
                {
                    // handle escaped double quotes ("")
                    if (inQuotes && i + 1 < text.Length && text[i + 1] == '"')
                    {
                        sb.Append('"');
                        i++; // skip escaped quote
                    }
                    else
                    {
                        inQuotes = !inQuotes;
                        sb.Append('"');
                    }
                }
                else if ((c == '\r' || c == '\n') && !inQuotes)
                {
                    // record separator (handle CRLF)
                    records.Add(sb.ToString());
                    sb.Clear();
                    if (c == '\r' && i + 1 < text.Length && text[i + 1] == '\n') i++;
                }
                else
                {
                    sb.Append(c);
                }
            }

            // add remaining
            if (sb.Length > 0)
                records.Add(sb.ToString());

            return records;
        }

        private static List<Dictionary<string, object?>> ParseJson(Stream ms)
        {
            var outList = new List<Dictionary<string, object?>>();
            ms.Position = 0;
            try
            {
                using var doc = JsonDocument.Parse(ms);
                var root = doc.RootElement;
                if (root.ValueKind == JsonValueKind.Array)
                {
                    foreach (var el in root.EnumerateArray())
                    {
                        if (el.ValueKind == JsonValueKind.Object)
                            outList.Add(JsonElementToDictionary(el));
                    }
                }
                else if (root.ValueKind == JsonValueKind.Object)
                {
                    // intentar arrays en propiedades conocidas
                    foreach (var prop in root.EnumerateObject())
                    {
                        if (prop.Value.ValueKind == JsonValueKind.Array)
                        {
                            foreach (var el in prop.Value.EnumerateArray())
                            {
                                if (el.ValueKind == JsonValueKind.Object)
                                    outList.Add(JsonElementToDictionary(el));
                            }
                            if (outList.Count > 0) return outList;
                        }
                    }

                    // fallback: mapear la raíz si es objeto con propiedades útiles
                    outList.Add(JsonElementToDictionary(root));
                }
            }
            catch
            {
                // intentar NDJSON (una línea por objeto)
                ms.Position = 0;
                using var sr = new StreamReader(ms, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, leaveOpen: true);
                string? line;
                while ((line = sr.ReadLine()) != null)
                {
                    if (string.IsNullOrWhiteSpace(line)) continue;
                    try
                    {
                        using var doc = JsonDocument.Parse(line);
                        if (doc.RootElement.ValueKind == JsonValueKind.Object)
                            outList.Add(JsonElementToDictionary(doc.RootElement));
                    }
                    catch { continue; }
                }
            }

            return outList;
        }

        private static Dictionary<string, object?> JsonElementToDictionary(JsonElement el)
        {
            var dict = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            foreach (var prop in el.EnumerateObject())
            {
                dict[prop.Name] = JsonValueToClr(prop.Value);
            }
            return dict;
        }

        private static object? JsonValueToClr(JsonElement v)
        {
            return v.ValueKind switch
            {
                JsonValueKind.Null => null,
                JsonValueKind.Undefined => null,
                JsonValueKind.String => v.GetString(),
                JsonValueKind.Number => v.TryGetInt64(out var l) ? (object)l : (v.TryGetDecimal(out var d) ? (object)d : v.GetDouble()),
                JsonValueKind.True => true,
                JsonValueKind.False => false,
                JsonValueKind.Object => JsonElementToDictionary(v),
                JsonValueKind.Array => v.GetRawText(), // serializar arrays como JSON string para simplicidad
                _ => v.GetRawText()
            };
        }

        private static List<Dictionary<string, object?>> ParseXml(XDocument doc)
        {
            var outList = new List<Dictionary<string, object?>>();
            if (doc.Root == null) return outList;

            // buscar nodos que parezcan registros: hijos del root u otros con elementos hijos
            var candidateNodes = doc.Root.Elements().Where(e => e.HasElements).ToList();
            if (!candidateNodes.Any()) candidateNodes = doc.Root.Elements().ToList();

            foreach (var el in candidateNodes)
            {
                var d = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

                // atributos
                foreach (var at in el.Attributes())
                    d[at.Name.LocalName] = at.Value;

                // elementos hijos
                foreach (var ch in el.Elements())
                {
                    if (ch.HasElements)
                        d[ch.Name.LocalName] = ch.ToString();
                    else
                        d[ch.Name.LocalName] = ch.Value;
                }

                // si no hay claves, intentar usar el nombre del nodo como columna 'Value'
                if (d.Count == 0)
                    d[el.Name.LocalName] = el.Value;

                outList.Add(d);
            }

            return outList;
        }

        private static List<Dictionary<string, object?>> ParseLines(List<string> lines, char separator, bool hasHeader)
        {
            var outList = new List<Dictionary<string, object?>>();
            int start = 0;
            List<string>? headers = null;

            if (hasHeader)
            {
                headers = ParseLine(lines[0], separator).Select(h => string.IsNullOrWhiteSpace(h) ? "Column" : h.Trim()).ToList();
                start = 1;
            }

            // si no hay header, crear Column1..N desde la primera línea válida
            if (headers == null)
            {
                if (lines.Count == 0) return outList;
                var sample = ParseLine(lines[start], separator);
                headers = Enumerable.Range(1, sample.Count).Select(i => $"Column{i}").ToList();
            }

            for (int i = start; i < lines.Count; i++)
            {
                // intentamos parsear la "línea lógica", y si quedan menos columnas que el header,
                // vamos concatenando las siguientes líneas hasta completar o agotarlas.
                string current = lines[i];
                var parts = ParseLine(current, separator).ToList();
                int j = i;
                while (parts.Count < headers.Count && j + 1 < lines.Count)
                {
                    j++;
                    // añadir backslash-n real entre registros combinados para no perder comillas/sep
                    current = current + "\n" + lines[j];
                    parts = ParseLine(current, separator).ToList();

                    // Si tras concatenar la siguiente línea conseguimos suficientes columnas, salimos.
                    if (parts.Count >= headers.Count) break;
                }

                // si se fusionaron líneas avanzamos el índice principal
                if (j > i) i = j;

                // Caso especial: línea completa envuelta entre comillas (usar la línea original)
                if (parts.Count == 1)
                {
                    var original = current ?? string.Empty;
                    if (original.Length > 1 && original.StartsWith("\"") && original.EndsWith("\""))
                    {
                        var inner = original.Substring(1, original.Length - 2).Replace("\"\"", "\"");
                        parts = ParseLine(inner, separator).ToList();
                    }
                }

                // Si sigue siendo más corta, rellenar con vacíos
                if (parts.Count < headers.Count)
                {
                    while (parts.Count < headers.Count) parts.Add(string.Empty);
                }

                var dict = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
                for (int c = 0; c < headers.Count; c++)
                {
                    var key = headers[c] ?? $"Column{c + 1}";
                    var val = c < parts.Count ? parts[c] : string.Empty;
                    dict[key] = string.IsNullOrWhiteSpace(val) ? null : (object)val;
                }

                outList.Add(dict);
            }

            return outList;
        }

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
            var parts = ParseLine(line, separator);
            if (parts.Count <= 1) return false;

            bool anyLetters = parts.Any(p => Regex.IsMatch(p ?? string.Empty, @"[A-Za-zÁÉÍÓÚáéíóúÑñ]"));
            bool allNumeric = parts.All(p =>
            {
                if (string.IsNullOrWhiteSpace(p)) return false;
                var s = p.Trim();
                return decimal.TryParse(s, NumberStyles.Number, CultureInfo.InvariantCulture, out _) || decimal.TryParse(s, NumberStyles.Number, CultureInfo.CurrentCulture, out _);
            });

            return anyLetters && !allNumeric;
        }

        private static List<string> ParseLine(string line, char separator)
        {
            var result = new List<string>();
            if (line == null) return result;

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
    }
}