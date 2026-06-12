<#
.SYNOPSIS
Script pour exécuter le job Spark Batch ETL périodiquement.
.DESCRIPTION
Ce script lance le script Python batch_writer.py. Il peut être planifié via le Planificateur de tâches Windows (Task Scheduler) pour s'exécuter toutes les heures.
#>

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Definition
$BatchWriterPath = Join-Path $ScriptDir "batch_writer.py"

# Chemin vers l'exécutable Python de l'environnement virtuel du projet
$ProjectRoot = Split-Path -Parent $ScriptDir
$PythonExe = Join-Path $ProjectRoot "venv\Scripts\python.exe"

# Si l'environnement virtuel n'existe pas, on utilise le python système
if (-Not (Test-Path $PythonExe)) {
    Write-Host "Environnement virtuel non trouvé dans $ProjectRoot\venv. Utilisation de python par défaut."
    $PythonExe = "python"
}

Write-Host "========================================"
Write-Host "Démarrage du Job Spark Batch ETL..."
Write-Host "Heure : $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
Write-Host "========================================"

# Exécution du script ETL
& $PythonExe $BatchWriterPath

Write-Host "========================================"
Write-Host "Fin de l'exécution du Job."
Write-Host "Vérifiez le fichier batch_logs.csv pour plus de détails."
Write-Host "========================================"
