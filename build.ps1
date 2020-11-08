dotnet tool install -g Cake.Tool
dotnet tool install -g Paket


paket install
dotnet cake build.cake -- $args