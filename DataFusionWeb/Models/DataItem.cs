namespace DataFusionArenaWeb.Models
{
    public class DataItem
    {
        public string Id { get; set; } = string.Empty;
        public string Nombre { get; set; } = string.Empty;
        public string Categoria { get; set; } = string.Empty;
        public decimal Valor { get; set; }
        public int Cantidad { get; set; } = 0;
        public decimal PrecioUnitario { get; set; } = 0m;
    }
}