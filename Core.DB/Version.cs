partial class GitVersion
{
    public const string Company = "Sequel Systems Inc.";

    public const string Copyright = "Copyright © 2020";

    public const string AssemblyVersion = ThisAssembly.Git.BaseVersion.Major + "." + ThisAssembly.Git.BaseVersion.Minor + "." + ThisAssembly.Git.BaseVersion.Patch;

    private const string BaseFullVersion = AssemblyVersion + "+" + ThisAssembly.Git.Commits + "-" + ThisAssembly.Git.Branch + "-" + ThisAssembly.Git.Commit;

#if __86__
    public const string FullVersion = BaseFullVersion + "-x86";
#elif __64__
    public const string FullVersion = BaseFullVersion + "-x64";
#else
    public const string FullVersion = BaseFullVersion + "-MSIL";
#endif

}