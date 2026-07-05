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

Write-Host "Atualizacao protegida da base dev/jean."
Write-Host "Git: $git"
Write-Host ""

$branch = (& $git branch --show-current).Trim()
if (-not $branch) {
    Write-Host "Nao foi possivel identificar a branch atual."
    exit 1
}

Write-Host "Branch atual: $branch"
Write-Host ""
& $git status --short --branch
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

$dirty = (& $git status --porcelain)
if ($dirty) {
    Write-Host ""
    Write-Host "Atualizacao bloqueada: existem alteracoes locais no worktree."
    Write-Host "Commit, reverta ou preserve essas alteracoes antes de atualizar dev/jean."
    Write-Host "Nenhuma branch foi trocada e nenhuma alteracao local foi descartada."
    exit 2
}

if ($branch -ne "dev/jean") {
    Write-Host ""
    Write-Host "A branch atual nao e dev/jean."
    Write-Host "Fluxo recomendado: task/* -> front/* -> dev/jean."
    Write-Host "Para sincronizar dev/jean, o script precisa trocar de branch."
    $confirm = Read-Host "Digite DEV-JEAN para trocar para dev/jean e atualizar"
    if ($confirm -ne "DEV-JEAN") {
        Write-Host "Atualizacao cancelada pelo usuario. Nenhuma alteracao foi feita."
        exit 3
    }
}

Write-Host ""
Write-Host "Buscando atualizacoes do repositorio remoto..."
& $git fetch origin
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

if ($branch -ne "dev/jean") {
    Write-Host ""
    Write-Host "Trocando para dev/jean..."
    & $git switch dev/jean
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

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
Write-Host "Base dev/jean atualizada com seguranca."
exit 0
