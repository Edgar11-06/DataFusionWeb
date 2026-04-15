using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using DataFusionArenaWeb.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllersWithViews();
builder.Services.AddRazorPages();

// Registrar provider para cadena en runtime
builder.Services.AddSingleton<ConnectionStringProvider>();

// Registrar el almacén en memoria existente (registrar la implementación concreta para inyección directa)
builder.Services.AddSingleton<DataFusionArenaWeb.Services.InMemoryDataStore>();

// Registrar el router que decide entre memoria y SQL
builder.Services.AddSingleton<DataStoreRouter>();
// Exponer router como la implementación principal de IInMemoryDataStore
builder.Services.AddSingleton<DataFusionArenaWeb.Services.IInMemoryDataStore>(sp => sp.GetRequiredService<DataStoreRouter>());

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseDeveloperExceptionPage();
}
else
{
    app.UseExceptionHandler("/Home/Error");
    app.UseHsts();
}

app.UseHttpsRedirection();
app.UseStaticFiles();
app.UseRouting();

app.MapControllerRoute(
    name: "default",
    pattern: "{controller=Items}/{action=Index}/{id?}");

app.MapRazorPages();

app.Run();

