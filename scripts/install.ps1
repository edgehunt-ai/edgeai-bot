[CmdletBinding()]
param(
    [string]$Version = "latest"
)

$ErrorActionPreference = "Stop"

$Repo = "DODOEX/edgeai"
$BinName = "edgeai"
$InstallDir = Join-Path $HOME ".edgeai\bin"
$TempDir = Join-Path ([System.IO.Path]::GetTempPath()) ("edgeai-install-" + [System.Guid]::NewGuid().ToString("N"))

function Remove-TempDir {
    if (Test-Path $TempDir) {
        Remove-Item -Recurse -Force $TempDir
    }
}

trap {
    Remove-TempDir
    throw
}

function Get-Platform {
    $arch = [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture
    switch ($arch) {
        "X64" { return "windows-x86_64" }
        default { throw "Unsupported architecture: $arch" }
    }
}

function Get-LatestVersion {
    $release = Invoke-RestMethod -Uri "https://api.github.com/repos/$Repo/releases/latest"
    if (-not $release.tag_name) {
        throw "Failed to resolve latest release version"
    }
    return $release.tag_name
}

function Get-InstalledVersion {
    $exePath = Join-Path $InstallDir "$BinName.exe"
    if (-not (Test-Path $exePath)) {
        return ""
    }

    try {
        $raw = & $exePath --version 2>$null
        if ($LASTEXITCODE -ne 0 -or -not $raw) {
            return ""
        }
        $parts = $raw -split "\s+"
        if ($parts.Length -ge 2) {
            return $parts[1]
        }
        return ""
    } catch {
        return ""
    }
}

function Add-InstallDirToUserPath {
    $userPath = [Environment]::GetEnvironmentVariable("Path", "User")
    $entries = @()

    if ($userPath) {
        $entries = $userPath -split ";" | Where-Object { $_ }
    }

    foreach ($entry in $entries) {
        if ($entry.TrimEnd('\') -ieq $InstallDir.TrimEnd('\')) {
            Write-Host "PATH already configured in user environment"
            return
        }
    }

    $newPath = if ($userPath) { "$userPath;$InstallDir" } else { $InstallDir }
    [Environment]::SetEnvironmentVariable("Path", $newPath, "User")
    Write-Host "Added $InstallDir to user PATH"
}

function Install-Release {
    param(
        [string]$ResolvedVersion,
        [string]$Platform
    )

    $url = "https://github.com/$Repo/releases/download/$ResolvedVersion/$BinName-$Platform.zip"
    $zipPath = Join-Path $TempDir "$BinName.zip"
    $extractDir = Join-Path $TempDir "extract"

    Write-Host "Downloading $BinName $ResolvedVersion for $Platform..."
    try {
        Invoke-WebRequest -Uri $url -OutFile $zipPath
    } catch {
        Write-Warning "Failed to download from GitHub releases."
        Write-Host "Clone the repo and build from source on Windows:"
        Write-Host "  git clone https://github.com/$Repo.git"
        Write-Host "  cd edgeai"
        Write-Host "  cargo build --release"
        throw
    }

    New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
    New-Item -ItemType Directory -Force -Path $extractDir | Out-Null
    Expand-Archive -Path $zipPath -DestinationPath $extractDir -Force

    $exe = Get-ChildItem -Path $extractDir -Filter "$BinName.exe" -Recurse | Select-Object -First 1
    if (-not $exe) {
        throw "Failed to find $BinName.exe in downloaded archive"
    }

    Copy-Item $exe.FullName (Join-Path $InstallDir "$BinName.exe") -Force
    Write-Host "Installed to $(Join-Path $InstallDir "$BinName.exe")"
}

New-Item -ItemType Directory -Force -Path $TempDir | Out-Null

$platform = Get-Platform
$resolvedVersion = if ($Version -eq "latest") { Get-LatestVersion } else { $Version }
$installedVersion = Get-InstalledVersion

if ($installedVersion -and $resolvedVersion -and $installedVersion -eq $resolvedVersion) {
    Write-Host "$BinName $resolvedVersion is already up to date."
    Remove-TempDir
    exit 0
}

Install-Release -ResolvedVersion $resolvedVersion -Platform $platform
Write-Host ""
Add-InstallDirToUserPath

$currentPathEntries = $env:Path -split ";" | Where-Object { $_ }
$hasCurrentPath = $false
foreach ($entry in $currentPathEntries) {
    if ($entry.TrimEnd('\') -ieq $InstallDir.TrimEnd('\')) {
        $hasCurrentPath = $true
        break
    }
}

if (-not $hasCurrentPath) {
    Write-Host ""
    Write-Host "Open a new PowerShell or Command Prompt window to use edgeai."
    Write-Host "For the current PowerShell session, run:"
    Write-Host "  `$env:Path = `"$InstallDir;`$env:Path`""
}

Remove-TempDir
