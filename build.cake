#tool "nuget:?package=GitVersion.CommandLine"

var target = Argument("target", "Default");
var outputDir = "./artifacts/";

Task("Clean")
    .Does(() => {
        if (DirectoryExists(outputDir))
        {
            DeleteDirectory(outputDir, new DeleteDirectorySettings());
        }
    });



GitVersion versionInfo = null;
Task("Version")
    .Does(() => {
        GitVersion(new GitVersionSettings{
            UpdateAssemblyInfo = false,
            OutputType = GitVersionOutput.BuildServer
        });
        versionInfo = GitVersion(new GitVersionSettings{ OutputType = GitVersionOutput.Json });
		versionInfo.NuGetVersion = "4.5.1-custom33";
    });

Task("Restore")
    .IsDependentOn("Version")
    .Does(() => {
        DotNetRestore("src", new DotNetRestoreSettings() {
            ArgumentCustomization = args => args.Append("/p:Version=" + versionInfo.NuGetVersion)
        });
    });

Task("Build")
    .IsDependentOn("Clean")
    .IsDependentOn("Version")
    .IsDependentOn("Restore")
    .Does(() => {
        var settings =  new MSBuildSettings()
            .SetConfiguration("Release")
            .UseToolVersion(MSBuildToolVersion.VS2022)
            .WithProperty("Version", versionInfo.NuGetVersion)
            .WithProperty("PackageOutputPath", System.IO.Path.GetFullPath(outputDir))
            .WithTarget("Build")
            .WithTarget("Pack");

        MSBuild("./src/DbUp.sln", settings);
    });

Task("Package")
	.IsDependentOn("Build")
    .Does(() => {

        NuGetPack("./src/dbup/dbup.nuspec", new NuGetPackSettings() {
            OutputDirectory = System.IO.Path.GetFullPath(outputDir),
            Version = versionInfo.NuGetVersion
        });

        System.IO.File.WriteAllLines(outputDir + "artifacts", new[]
        {
            "core:dbup-core." + versionInfo.NuGetVersion + ".nupkg",
            "yellowbrick:dbup-yellowbrick." + versionInfo.NuGetVersion + ".nupkg"
        });

        if (AppVeyor.IsRunningOnAppVeyor)
        {
            foreach (var file in GetFiles(outputDir + "**/*"))
                AppVeyor.UploadArtifact(file.FullPath);
        }
    });

Task("Default")
    .IsDependentOn("Package");

RunTarget(target);