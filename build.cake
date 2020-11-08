var target = Argument("target", "Default");
var configuration = Argument("configuration", "Release");

Setup(context =>
{
    CleanUp();
});

Teardown(context =>
{
});

TaskTeardown(teardownContext =>
{
    
});

Task("Clean").Does(() =>
{
    CleanUp();
});

Task("Net46").IsDependentOn("Clean").Does(() =>
{
    Build("./Core.DB/Core.DB.csproj", "net46", "win-x64");
});


void CleanUp()
{
    var settings = new DotNetCoreCleanSettings
    {
        Configuration = configuration
    };
    
    DotNetCoreClean("./Core.sln", settings);
}

void Restore(string[] args)
{
    StartProcess("paket", new ProcessSettings{ Arguments = "update" });
    DotNetCoreRestore();
}

void Build(string project, string framework, string runtime)
{
    var settings = new DotNetCoreBuildSettings
    {
        Framework = framework,
        Runtime = runtime,
        Configuration = configuration,
        NoIncremental = true,
        NoRestore = true,
        MSBuildSettings = new DotNetCoreMSBuildSettings().WithProperty("nowarn", "7035")
    };

    DotNetCoreBuild(project, settings);
}

Task("Default").IsDependentOn("net46");

RunTarget(target);