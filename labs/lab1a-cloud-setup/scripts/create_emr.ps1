param(
    [string]$Student = "efcortesr",
    [string]$Year = "2026",
    [string]$Region = "us-east-1",
    [string]$KeyName = "st1630-lab1a",
    [string]$InstanceProfile = "EMR_EC2_DefaultRole",
    [string]$SubnetId = ""
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

$bucketName = "st1630-$Student-$Year"
$clusterName = "st1630-$Student-emr"
$tempDir = New-Item -ItemType Directory -Path (Join-Path ([System.IO.Path]::GetTempPath()) ([System.Guid]::NewGuid())) -Force

try {
    if (-not $SubnetId) {
        Write-Host "Buscando una subnet por defecto en $Region..."
        $SubnetId = Invoke-Aws ec2 describe-subnets `
            --filters "Name=default-for-az,Values=true" `
            --query "Subnets[0].SubnetId" `
            --output text `
            --region $Region

        if (-not $SubnetId -or $SubnetId -eq "None") {
            throw "No default subnet found. Pass -SubnetId subnet-xxxxxxxx."
        }
    }

    Write-Host "Verificando roles de servicio por defecto de EMR..."
    & aws emr create-default-roles *> $null

    $bootstrapPath = Join-Path $tempDir "bootstrap.sh"
    @'
#!/bin/bash
sudo pip3 install --quiet pandas pyarrow
'@ | Set-Content -Path $bootstrapPath -Encoding ascii

    Invoke-Aws s3 cp $bootstrapPath "s3://$bucketName/bootstrap/bootstrap.sh" | Out-Null

    Write-Host "Creando cluster EMR '$clusterName'..."
    $clusterId = Invoke-Aws emr create-cluster `
        --name $clusterName `
        --release-label "emr-6.15.0" `
        --applications "Name=Spark" "Name=Hadoop" "Name=Livy" "Name=JupyterEnterpriseGateway" `
        --instance-type "m5.xlarge" `
        --instance-count "2" `
        --service-role "EMR_DefaultRole" `
        --ec2-attributes "KeyName=$KeyName,InstanceProfile=$InstanceProfile,SubnetId=$SubnetId" `
        --log-uri "s3://$bucketName/logs/" `
        --bootstrap-actions "Path=s3://$bucketName/bootstrap/bootstrap.sh,Name=Instalar dependencias Python" `
        --region $Region `
        --query "ClusterId" --output text

    Write-Host "Cluster creado: $clusterId"
    Write-Host "Estado inicial:"
    Invoke-Aws emr describe-cluster `
        --cluster-id $clusterId `
        --region $Region `
        --query "Cluster.Status.State" --output text

    Write-Host ""
    Write-Host "Para volver a consultar el estado:"
    Write-Host "aws emr describe-cluster --cluster-id $clusterId --region $Region --query 'Cluster.Status.State' --output text"
    Write-Host ""
    Write-Host "APAGA el cluster cuando termines la Parte 5:"
    Write-Host "aws emr terminate-clusters --cluster-ids $clusterId --region $Region"
} finally {
    Remove-Item -LiteralPath $tempDir -Recurse -Force
}
