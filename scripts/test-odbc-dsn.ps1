# Simple script to test DuckDB ODBC driver installation on Windows

# Check if driver is registered
Write-Host "Checking if DuckDB ODBC driver is registered..."
if (Test-Path "HKLM:\SOFTWARE\ODBC\ODBCINST.INI\DuckDB Driver") {
    Write-Host "✅ DuckDB Driver is registered in the system"
    $driverInfo = Get-ItemProperty "HKLM:\SOFTWARE\ODBC\ODBCINST.INI\DuckDB Driver"
    Write-Host "Driver path: $($driverInfo.Driver)"
    
    # Optional: List all registered ODBC drivers
    Write-Host "All registered ODBC drivers:"
    if (Test-Path "HKLM:\SOFTWARE\ODBC\ODBCINST.INI\ODBC Drivers") {
        $driversProperty = Get-ItemProperty "HKLM:\SOFTWARE\ODBC\ODBCINST.INI\ODBC Drivers"
        $driversProperty.PSObject.Properties | Where-Object { $_.Name -ne "PSPath" -and $_.Name -ne "PSParentPath" -and $_.Name -ne "PSChildName" -and $_.Name -ne "PSDrive" -and $_.Name -ne "PSProvider" } | ForEach-Object {
            Write-Host "  $($_.Name): $($_.Value)"
        }
    }
    
    # Create a DSN for testing
    Write-Host "Creating a test DSN..."
    $dsn = "DuckDB_Test"
    $dsnPath = "HKLM:\SOFTWARE\ODBC\ODBC.INI\$dsn"
    
    if (!(Test-Path "HKLM:\SOFTWARE\ODBC\ODBC.INI")) {
        New-Item -Path "HKLM:\SOFTWARE\ODBC\ODBC.INI" -Force | Out-Null
    }
    
    if (!(Test-Path $dsnPath)) {
        New-Item -Path $dsnPath -Force | Out-Null
    }
    
    New-ItemProperty -Path $dsnPath -Name "Driver" -Value $driverInfo.Driver -PropertyType String -Force | Out-Null
    New-ItemProperty -Path $dsnPath -Name "Description" -Value "DuckDB Test DSN" -PropertyType String -Force | Out-Null
    New-ItemProperty -Path $dsnPath -Name "Database" -Value ":memory:" -PropertyType String -Force | Out-Null
    
    # Register DSN in ODBC Data Sources
    if (!(Test-Path "HKLM:\SOFTWARE\ODBC\ODBC.INI\ODBC Data Sources")) {
        New-Item -Path "HKLM:\SOFTWARE\ODBC\ODBC.INI\ODBC Data Sources" -Force | Out-Null
    }
    
    New-ItemProperty -Path "HKLM:\SOFTWARE\ODBC\ODBC.INI\ODBC Data Sources" -Name $dsn -Value "DuckDB Driver" -PropertyType String -Force | Out-Null
    Write-Host "✅ Test DSN '$dsn' created successfully"
    
    # Optional: Test the connection

    Write-Host "Testing connection to DuckDB..."
    try {
        Add-Type -Path "System.Data.dll"
        $conn = New-Object System.Data.Odbc.OdbcConnection("DSN=$dsn")
        $conn.Open()
        
        $cmd = New-Object System.Data.Odbc.OdbcCommand("SELECT 42 AS answer", $conn)
        $reader = $cmd.ExecuteReader()
        
        if ($reader.Read()) {
            $result = $reader.GetValue(0)
            Write-Host "Query result: $result"
            if ($result -eq 42) {
                Write-Host "✅ Connection test passed"
            } else {
                Write-Host "❌ Unexpected result: Expected 42, got $result"
                exit 1
            }
        }
        
        $reader.Close()
        $conn.Close()
    } 
    catch {
        Write-Host "❌ Connection test failed: $_"
        exit 1
    }

    
    Write-Host "✅ DuckDB ODBC driver testing complete"
    exit 0
} 
else {
    Write-Host "❌ DuckDB Driver is not registered in the system"
    
    # List any registered ODBC drivers for debugging
    Write-Host "Currently registered ODBC drivers:"
    if (Test-Path "HKLM:\SOFTWARE\ODBC\ODBCINST.INI\ODBC Drivers") {
        $driversProperty = Get-ItemProperty "HKLM:\SOFTWARE\ODBC\ODBCINST.INI\ODBC Drivers"
        $driversProperty.PSObject.Properties | Where-Object { $_.Name -ne "PSPath" -and $_.Name -ne "PSParentPath" -and $_.Name -ne "PSChildName" -and $_.Name -ne "PSDrive" -and $_.Name -ne "PSProvider" } | ForEach-Object {
            Write-Host "  $($_.Name): $($_.Value)"
        }
    } else {
        Write-Host "  No drivers found"
    }
    
    exit 1
}