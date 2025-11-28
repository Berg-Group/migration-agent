# PowerShell script to load .env and run table comparison
$envFile = "..\.env"

if (Test-Path $envFile) {
    Write-Host "Loading environment variables from: $envFile"
    Get-Content $envFile | ForEach-Object {
        if ($_ -match '^\s*([^#][^=]*)=(.*)$') {
            $key = $matches[1].Trim()
            $value = $matches[2].Trim()
            # Remove quotes if present
            if ($value -match '^["''](.*)["'']$') {
                $value = $matches[1]
            }
            [Environment]::SetEnvironmentVariable($key, $value, "Process")
        }
    }
} else {
    Write-Host "Error: .env file not found at $envFile"
    exit 1
}

# Run the Python comparison script with table name argument if provided
if ($args.Count -gt 0) {
    python tests\compare_tables.py $args[0]
} else {
    python tests\compare_tables.py
}

