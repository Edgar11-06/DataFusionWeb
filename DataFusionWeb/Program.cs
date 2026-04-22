using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using DataFusionArenaWeb.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllersWithViews();
builder.Services.AddRazorPages();

// provider y almacén en memoria
builder.Services.AddSingleton<ConnectionStringProvider>();
builder.Services.AddSingleton<IInMemoryDataStore, InMemoryDataStore>();
builder.Services.AddSingleton<DataStoreRouter>();

// registramos migrators para persistencia dinámica
builder.Services.AddTransient<SqlServerDataMigrator>();
builder.Services.AddTransient<PostgresDataMigrator>();
builder.Services.AddTransient<MariaDbDataMigrator>();

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

