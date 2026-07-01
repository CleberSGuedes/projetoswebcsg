$ErrorActionPreference = "Stop"
Set-Location -LiteralPath $PSScriptRoot

function Find-Git {
    $cmd = Get-Command git -ErrorAction SilentlyContinue
    if ($cmd) { return $cmd.Source }

    $candidates = @(
        "$env:ProgramFiles\Git\cmd\git.exe",
        "$env:ProgramFiles\Git\bin\git.exe",
        "${env:ProgramFiles(x86)}\Git\cmd\git.exe",
        "${env:ProgramFiles(x86)}\Git\bin\git.exe"
    )

    foreach ($candidate in $candidates) {
        if ($candidate -and (Test-Path -LiteralPath $candidate)) {
            return $candidate
        }
    }

    return $null
}

$git = Find-Git
if (-not $git) {
    Write-Host "Git nao encontrado neste Windows."
    Write-Host "Instale o Git ou abra este script em um terminal onde o comando git funcione."
    exit 1
}

Write-Host "Atualizando codigo do ambiente intermediario dev/jean."
Write-Host "Git: $git"
Write-Host ""

& $git status --short --branch
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host ""
Write-Host "Buscando atualizacoes do repositorio remoto..."
& $git fetch origin
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host ""
Write-Host "Garantindo branch dev/jean..."
& $git checkout dev/jean
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host ""
Write-Host "Aplicando atualizacoes sem sobrescrever alteracoes locais..."
& $git pull --ff-only origin dev/jean
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

$python = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
if (Test-Path -LiteralPath $python) {
    Write-Host ""
    Write-Host "Conferindo dependencias Python..."
    & $python -m pip install -r requirements.txt
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

Write-Host ""
Write-Host "Codigo intermediario atualizado."
exit 0
