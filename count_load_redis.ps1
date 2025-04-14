# Redis connection settings
$redisHost = "192.168.8.211"
$redisPort = "6379"
$redisPassword = "0ef1sJm19w3OKHiH"

function Send-RedisCommand {
    param (
        [string]$command
    )
    
    $client = New-Object System.Net.Sockets.TcpClient
    try {
        $client.Connect($redisHost, $redisPort)
        $stream = $client.GetStream()
        
        # Auth first
        $authCmd = "AUTH $redisPassword`r`n"
        $authBytes = [System.Text.Encoding]::ASCII.GetBytes($authCmd)
        $stream.Write($authBytes, 0, $authBytes.Length)
        
        # Read auth response
        $buffer = New-Object byte[] 1024
        $stream.Read($buffer, 0, $buffer.Length) | Out-Null
        
        # Send actual command
        $cmdBytes = [System.Text.Encoding]::ASCII.GetBytes("$command`r`n")
        $stream.Write($cmdBytes, 0, $cmdBytes.Length)
        
        # Read response
        $responseBuilder = New-Object System.Text.StringBuilder
        do {
            $bytesRead = $stream.Read($buffer, 0, $buffer.Length)
            $responseBuilder.Append([System.Text.Encoding]::ASCII.GetString($buffer, 0, $bytesRead))
        } while ($stream.DataAvailable)
        
        return $responseBuilder.ToString()
    }
    finally {
        if ($stream) { $stream.Dispose() }
        if ($client) { $client.Dispose() }
    }
}

function Get-IndexCount {
    param (
        [string]$indexName
    )
    
    try {
        $info = Send-RedisCommand "FT.INFO $indexName"
        $lines = $info -split "`n"
        foreach ($line in $lines) {
            if ($line -match "num_docs") {
                $count = $line -replace '.*?([0-9]+).*','$1'
                return $count
            }
        }
        return "0"
    } catch {
        Write-Warning "Failed to get count for $indexName: $_"
        return "0"
    }
}

try {
    Write-Host "Starting Redis index count..."
    
    # Get counts for each index
    $counts = @{
        poiCount = Get-IndexCount "poi-idx"
        entryCount = Get-IndexCount "entry-poi-idx"
        evseCount = Get-IndexCount "evse-power-idx"
        timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    }
    
    # Output results
    Write-Host "Redis index counting completed successfully"
    Write-Host "POI Index Count: $($counts.poiCount)"
    Write-Host "Entry POI Index Count: $($counts.entryCount)"
    Write-Host "EVSE Power Index Count: $($counts.evseCount)"
    Write-Host "Count Time: $($counts.timestamp)"

    # Return counts object
    $counts

} catch {
    Write-Error "Error occurred: $($_.Exception.Message)"
    exit 1
}