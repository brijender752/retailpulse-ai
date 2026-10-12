param(
    [string]$Category,
    [ValidateRange(0, 100)]
    [int]$Percent
)

$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
$statusPath = Join-Path $projectRoot 'project-status.json'
$readmePath = Join-Path $projectRoot 'README.md'
$utf8 = New-Object System.Text.UTF8Encoding($false)
$statuses = Get-Content -LiteralPath $statusPath -Raw | ConvertFrom-Json

if ($Category) {
    if (-not $PSBoundParameters.ContainsKey('Percent')) {
        throw 'Provide -Percent when updating a category.'
    }
    $selected = @($statuses | Where-Object { $_.name -eq $Category })
    if ($selected.Count -ne 1) {
        throw "Unknown or duplicate category: $Category"
    }
    $selected[0].percent = $Percent
} elseif ($PSBoundParameters.ContainsKey('Percent')) {
    throw 'Provide -Category when updating a percentage.'
}

$rows = foreach ($status in $statuses) {
    if ($status.percent -isnot [int] -or $status.percent -lt 0 -or $status.percent -gt 100) {
        throw "Invalid percentage for $($status.name)"
    }
    $filled = [int][Math]::Floor($status.percent / 5)
    $bar = ('#' * $filled) + ('-' * (20 - $filled))
    '| {0} | **{1}%** | `{2}` | {3} |' -f $status.name, $status.percent, $bar, $status.scope
}

$start = '<!-- project-status:start -->'
$end = '<!-- project-status:end -->'
$section = @(
    $start
    '## Overall project status'
    ''
    'Completion estimates maintained as project milestones are completed.'
    ''
    '| Area | Complete | Progress | Scope |'
    '| --- | ---: | --- | --- |'
    $rows
    ''
    'Update a percentage and regenerate this section from the repository root:'
    ''
    '```powershell'
    './scripts/update_project_status.ps1 -Category "GenAI" -Percent 70'
    '```'
    ''
    'The source of truth is [project-status.json](project-status.json). After editing it directly, run `./scripts/update_project_status.ps1` to refresh this section.'
    $end
) -join "`n"

$readme = [System.IO.File]::ReadAllText($readmePath)
$pattern = '(?s)' + [regex]::Escape($start) + '.*?' + [regex]::Escape($end)
if ($readme.Contains($start) -and $readme.Contains($end)) {
    $readme = [regex]::Replace($readme, $pattern, [System.Text.RegularExpressions.MatchEvaluator]{ param($match) $section })
} elseif ($readme.Contains($start) -or $readme.Contains($end)) {
    throw 'README status section has an incomplete marker pair.'
} else {
    $anchor = '## Start Docker containers step by step'
    if (-not $readme.Contains($anchor)) { throw 'README insertion point not found.' }
    $readme = $readme.Replace($anchor, $section + "`n`n" + $anchor)
}

if ($Category) {
    [System.IO.File]::WriteAllText($statusPath, ($statuses | ConvertTo-Json -Depth 5) + "`n", $utf8)
}
[System.IO.File]::WriteAllText($readmePath, $readme, $utf8)
Write-Output 'README project status refreshed.'
