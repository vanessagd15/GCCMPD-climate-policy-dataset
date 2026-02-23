# PowerShell script for searching code in the GCCMPD climate policy dataset
# Usage examples:
# .\search_code.ps1 "requests|selenium" "*.py"
# .\search_code.ps1 "import.*pandas" "*.py"

param(
    [string]$SearchPattern = "",
    [string]$FilePattern = "*.*",
    [string]$Directory = "."
)

if ($SearchPattern -eq "") {
    Write-Host "Usage: .\search_code.ps1 'pattern' 'file_pattern' 'directory'"
    Write-Host "Example: .\search_code.ps1 'requests|selenium' '*.py' './code and files/crawl'"
    exit
}

Write-Host "Searching for pattern: $SearchPattern"
Write-Host "In files matching: $FilePattern"
Write-Host "Directory: $Directory"
Write-Host "=" * 50

try {
    $results = Select-String -Path (Join-Path $Directory $FilePattern) -Pattern $SearchPattern -AllMatches
    
    if ($results) {
        foreach ($result in $results) {
            Write-Host "File: $($result.Filename)" -ForegroundColor Green
            Write-Host "Line $($result.LineNumber): $($result.Line.Trim())" -ForegroundColor Yellow
            Write-Host ""
        }
        Write-Host "Found $($results.Count) matches" -ForegroundColor Cyan
    } else {
        Write-Host "No matches found" -ForegroundColor Red
    }
} catch {
    Write-Host "Error: $($_.Exception.Message)" -ForegroundColor Red
}
