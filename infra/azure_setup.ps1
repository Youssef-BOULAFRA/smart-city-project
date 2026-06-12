<#
.SYNOPSIS
Script pour provisionner l'infrastructure Azure pour le projet Smart City (Data Lake et IoT Hub).
.DESCRIPTION
Assurez-vous d'être connecté à Azure CLI ("az login") avant d'exécuter ce script.
#>

Set-StrictMode -Version Latest

$ProjectRoot = Split-Path -Parent $PSScriptRoot
$ResourceGroup = "SmartCityRG_P4_E2" # Nouveau groupe dans une région autorisée
$Location = "eastus2"
$IotHubName = "SmartCityHubP4E2" # Les noms IoT Hub et Storage doivent être uniques mondialement ! Modifiez-le si besoin.
$StorageAccountName = "smartcitylakep4e2"
$EnvFilePath = Join-Path $ProjectRoot "iot_simulation\.env"
$EnvExamplePath = Join-Path $ProjectRoot "iot_simulation\.env.example"

$DeviceIds = @(
    "pollution_centre_ville",
    "pollution_zone_industrielle",
    "pollution_residentiel",
    "pollution_peripherie",
    "traffic_centre_ville",
    "traffic_zone_industrielle",
    "traffic_residentiel",
    "traffic_peripherie",
    "lighting_centre_ville",
    "lighting_zone_industrielle",
    "lighting_residentiel",
    "lighting_peripherie",
    "waste_centre_ville",
    "waste_zone_industrielle",
    "waste_residentiel",
    "waste_peripherie"
)

function Write-EnvFile {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path,
        [Parameter(Mandatory = $true)]
        [string[]]$Lines
    )

    $directory = Split-Path -Parent $Path
    if (-not (Test-Path $directory)) {
        New-Item -ItemType Directory -Path $directory -Force | Out-Null
    }

    Set-Content -Path $Path -Value $Lines -Encoding utf8
}

Write-Host "1. Création du Groupe de Ressources..."
az group create --name $ResourceGroup --location $Location

Write-Host "2. Création du IoT Hub (Niveau Gratuit F1)..."
az extension add --name azure-iot --yes | Out-Null
az iot hub create --name $IotHubName --resource-group $ResourceGroup --sku F1
az iot hub wait --name $IotHubName --resource-group $ResourceGroup --created

Write-Host "3. Création du compte de stockage (Data Lake Gen2)..."
az storage account create --name $StorageAccountName --resource-group $ResourceGroup `
    --sku Standard_LRS --kind StorageV2 --hns true

Write-Host "4. Récupération de la clé du compte de stockage..."
$StorageKey = (az storage account keys list --resource-group $ResourceGroup --account-name $StorageAccountName --query "[0].value" --output tsv)

Write-Host "5. Création des conteneurs (raw, processed, aggregated)..."
# Nous devons utiliser la clé de stockage pour créer les conteneurs
az storage container create --name "raw" --account-name $StorageAccountName --account-key $StorageKey
az storage container create --name "processed" --account-name $StorageAccountName --account-key $StorageKey
az storage container create --name "aggregated" --account-name $StorageAccountName --account-key $StorageKey

Write-Host "6. Création des devices IoT et récupération des connection strings..."
$DeviceConnectionStrings = @{}

foreach ($DeviceId in $DeviceIds) {
    $existingDevice = az iot hub device-identity show --hub-name $IotHubName --device-id $DeviceId --resource-group $ResourceGroup --query "deviceId" --output tsv 2>$null

    if (-not $existingDevice) {
        az iot hub device-identity create --hub-name $IotHubName --device-id $DeviceId --resource-group $ResourceGroup
    }

    $ConnectionString = (az iot hub device-identity connection-string show --hub-name $IotHubName --device-id $DeviceId --resource-group $ResourceGroup --output tsv)
    $DeviceConnectionStrings[$DeviceId] = $ConnectionString
}

Write-Host "7. Récupération de la connection string du hub pour le bridge IoT Hub → Kafka..."
$EventHubConnectionString = (az iot hub connection-string show --hub-name $IotHubName --policy-name service --resource-group $ResourceGroup --output tsv)

Write-Host "8. Génération du fichier .env..."
$EnvLines = @(
    "# Variables partagées pour le projet Smart City",
    "STORAGE_ACCOUNT_NAME=$StorageAccountName",
    "STORAGE_ACCOUNT_KEY=$StorageKey",
    "EVENT_HUB_CONNECTION_STRING=$EventHubConnectionString",
    ""
)

foreach ($DeviceId in $DeviceIds) {
    $EnvLines += "$DeviceId=$($DeviceConnectionStrings[$DeviceId])"
}

Write-EnvFile -Path $EnvFilePath -Lines $EnvLines
Write-EnvFile -Path $EnvExamplePath -Lines ($EnvLines | ForEach-Object { if ($_ -match '^([^=]+)=') { "$($Matches[1])=" } else { $_ } })

Write-Host "======================================================"
Write-Host "INFRASTRUCTURE CRÉÉE AVEC SUCCÈS !"
Write-Host "======================================================"
Write-Host "Voici les informations à ajouter/modifier dans votre fichier .env :"
Write-Host "STORAGE_ACCOUNT_NAME=$StorageAccountName"
Write-Host "STORAGE_ACCOUNT_KEY=$StorageKey"
Write-Host "EVENT_HUB_CONNECTION_STRING=..."
Write-Host "======================================================"
Write-Host "Le fichier .env a été généré dans : $EnvFilePath"
Write-Host "Le template .env.example a été généré dans : $EnvExamplePath"
