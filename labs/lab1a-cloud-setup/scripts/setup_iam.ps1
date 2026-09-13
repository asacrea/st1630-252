param(
    [string]$Student = "efcortesr",
    [string]$Year = "2026"
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
$roleName = "EMR_EC2_${Student}_role"
$profileName = "EMR_EC2_${Student}_profile"
$policyName = "st1630-$Student-s3-min-privilegio"
$tempDir = New-Item -ItemType Directory -Path (Join-Path ([System.IO.Path]::GetTempPath()) ([System.Guid]::NewGuid())) -Force

try {
    $trustPolicyPath = Join-Path $tempDir "trust-policy.json"
    $correctPolicyPath = Join-Path $tempDir "policy-correcta.json"
    $incorrectPolicyPath = Join-Path $tempDir "policy-incorrecta-NO-USAR.json"

    @'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": { "Service": "ec2.amazonaws.com" },
      "Action": "sts:AssumeRole"
    }
  ]
}
'@ | Set-Content -Path $trustPolicyPath -Encoding utf8

    Write-Host "Rol objetivo: $roleName (bucket: s3://$bucketName)"

    if (Test-AwsSuccess iam get-role --role-name $roleName) {
        Write-Host "El rol $roleName ya existe; se omite la creacion."
    } else {
        Write-Host "Creando rol $roleName..."
        Invoke-Aws iam create-role `
            --role-name $roleName `
            --assume-role-policy-document "file://$trustPolicyPath" | Out-Null
    }

    if (Test-AwsSuccess iam get-instance-profile --instance-profile-name $profileName) {
        Write-Host "El instance profile $profileName ya existe; se omite la creacion."
    } else {
        Write-Host "Creando instance profile $profileName..."
        Invoke-Aws iam create-instance-profile --instance-profile-name $profileName | Out-Null
    }

    $attachedRole = Invoke-Aws iam get-instance-profile `
        --instance-profile-name $profileName `
        --query "InstanceProfile.Roles[?RoleName=='$roleName'].RoleName" `
        --output text

    if (-not $attachedRole) {
        Invoke-Aws iam add-role-to-instance-profile `
            --instance-profile-name $profileName `
            --role-name $roleName | Out-Null
        Start-Sleep -Seconds 10
    }

    @"
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AccesoObjetosBucketPropio",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": "arn:aws:s3:::$bucketName/*"
    },
    {
      "Sid": "ListarBucketPropio",
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::$bucketName"
    }
  ]
}
"@ | Set-Content -Path $correctPolicyPath -Encoding utf8

    @'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "NO_USAR_excesivamente_permisivo",
      "Effect": "Allow",
      "Action": "s3:*",
      "Resource": "*"
    }
  ]
}
'@ | Set-Content -Path $incorrectPolicyPath -Encoding utf8

    Write-Host "Referencia guardada (NO aplicada): $incorrectPolicyPath"
    Get-Content $incorrectPolicyPath
    Write-Host ""

    $accountId = Invoke-Aws sts get-caller-identity --query Account --output text
    $policyArn = "arn:aws:iam::${accountId}:policy/$policyName"

    if (Test-AwsSuccess iam get-policy --policy-arn $policyArn) {
        Write-Host "La politica $policyName ya existe; se omite la creacion."
    } else {
        Write-Host "Creando politica $policyName..."
        Invoke-Aws iam create-policy `
            --policy-name $policyName `
            --policy-document "file://$correctPolicyPath" | Out-Null
    }

    Write-Host "Adjuntando politica al rol..."
    Invoke-Aws iam attach-role-policy --role-name $roleName --policy-arn $policyArn | Out-Null

    $roleArn = "arn:aws:iam::${accountId}:role/$roleName"

    Write-Host ""
    Write-Host "=== Simulacion: puede escribir en su propio bucket? (debe ser 'allowed') ==="
    Invoke-Aws iam simulate-principal-policy `
        --policy-source-arn $roleArn `
        --action-names "s3:PutObject" `
        --resource-arns "arn:aws:s3:::$bucketName/bronze/test.txt" `
        --query "EvaluationResults[0].EvalDecision" --output text

    Write-Host ""
    Write-Host "=== Simulacion: puede borrar OTRO bucket de la cuenta? (debe ser 'implicitDeny') ==="
    Invoke-Aws iam simulate-principal-policy `
        --policy-source-arn $roleArn `
        --action-names "s3:DeleteBucket" `
        --resource-arns "arn:aws:s3:::$bucketName" `
        --query "EvaluationResults[0].EvalDecision" --output text

    Write-Host ""
    Write-Host "Listo. Instance profile para EMR: $profileName"
    Write-Host "Siguiente paso: .\scripts\create_emr.ps1"
} finally {
    Remove-Item -LiteralPath $tempDir -Recurse -Force
}
