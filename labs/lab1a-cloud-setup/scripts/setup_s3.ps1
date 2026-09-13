param(
    [string]$Student = "efcortesr",
    [string]$Year = "2026",
    [string]$Region = "us-east-1"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Invoke-Aws {
    param(
        [Parameter(ValueFromRemainingArguments = $true)]
        [string[]]$AwsArgs
    )

    $output = & aws @AwsArgs
    if ($LASTEXITCODE -ne 0) {
        throw "aws $($AwsArgs -join ' ') failed with exit code $LASTEXITCODE"
    }

    return $output
}

function Test-AwsSuccess {
    param(
        [Parameter(ValueFromRemainingArguments = $true)]
        [string[]]$AwsArgs
    )

    try {
        & aws @AwsArgs *> $null
        return $LASTEXITCODE -eq 0
    } catch {
        return $false
    }
}

$bucketName = "st1630-$Student-$Year"
$dataDir = Resolve-Path (Join-Path $PSScriptRoot "..\datos")
$parquetPath = Join-Path $dataDir "prueba_parquet.parquet"
$csvPath = Join-Path $dataDir "prueba_csv.csv"

if (-not (Test-Path $parquetPath) -or -not (Test-Path $csvPath)) {
    throw "Missing generated data files. Run: python .\datos\generar_datos.py"
}

Write-Host "Bucket objetivo: s3://$bucketName (region $Region)"

if (Test-AwsSuccess s3api head-bucket --bucket $bucketName) {
    Write-Host "El bucket ya existe; se omite la creacion."
} else {
    Write-Host "Creando bucket..."
    if ($Region -eq "us-east-1") {
        Invoke-Aws s3api create-bucket --bucket $bucketName --region $Region | Out-Null
    } else {
        Invoke-Aws s3api create-bucket --bucket $bucketName --region $Region `
            --create-bucket-configuration "LocationConstraint=$Region" | Out-Null
    }
}

Write-Host "Bloqueando acceso publico..."
Invoke-Aws s3api put-public-access-block `
    --bucket $bucketName `
    --public-access-block-configuration "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true" | Out-Null

Write-Host "Creando estructura de prefijos (bronze/, silver/, gold/)..."
foreach ($layer in @("bronze", "silver", "gold")) {
    Invoke-Aws s3api put-object --bucket $bucketName --key "$layer/" | Out-Null
}

Write-Host "Subiendo archivos de prueba a bronze/ventas/..."
Invoke-Aws s3 cp $parquetPath "s3://$bucketName/bronze/ventas/prueba_parquet.parquet" | Out-Null
Invoke-Aws s3 cp $csvPath "s3://$bucketName/bronze/ventas/prueba_csv.csv" | Out-Null

Write-Host ""
Write-Host "=== Estructura del datalake (s3://$bucketName) ==="
Invoke-Aws s3 ls "s3://$bucketName" --recursive --human-readable --summarize

Write-Host ""
Write-Host "Listo. Bucket: s3://$bucketName"
Write-Host "Siguiente paso: .\scripts\setup_iam.ps1"
