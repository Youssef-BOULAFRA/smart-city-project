$ErrorActionPreference = 'Stop'

$projectRoot = Split-Path -Parent $PSScriptRoot
$linkRoot = 'C:\smartcity-project-link'
$linkCreated = $false

if (-not (Test-Path $linkRoot)) {
    try {
        New-Item -ItemType Junction -Path $linkRoot -Target $projectRoot | Out-Null
    }
    catch {
        New-PSDrive -Name 'smartcity' -PSProvider FileSystem -Root $projectRoot | Out-Null
        $linkRoot = 'smartcity:\'
    }
    $linkCreated = $true
}

try {
    & "$linkRoot\venv\Scripts\python.exe" "$linkRoot\streaming\spark_streaming.py"
}
finally {
    if ($linkCreated -and (Test-Path 'C:\smartcity-project-link')) {
        try {
            Remove-Item -Path 'C:\smartcity-project-link' -Force
        }
        catch {
            Remove-PSDrive -Name 'smartcity' -ErrorAction SilentlyContinue
        }
    }
}
