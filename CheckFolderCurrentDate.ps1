# parameters
$folderPath = "D:\Ftp"

# functions
function Check-FolderByDate {
    param (
        [string]$folderPath
    )

    $currentDate = (Get-Date).ToString("yyyyMMdd")
    $folderPath = "$folderPath\$currentDate"

    if (Test-Path -Path $folderPath) {
        Write-Host "Folder $folderPath exists."
        return $true
    } else {
        Write-Host "Folder $folderPath does not exist."
        return $false
    }
}

function Check-FolderForJsonFiles {
    param (
        [string]$folderPath
    )

    if (-Not (Test-Path -Path $folderPath)) {
        Write-Host "Folder $folderPath does not exist."
        return $false
    }

    $jsonFiles = Get-ChildItem -Path $folderPath -Filter *.json

    if ($jsonFiles.Count -eq 0) {
        Write-Host "No JSON files found in $folderPath."
        return $false
    }

    foreach ($file in $jsonFiles) {
        try {
            $content = Get-Content -Path $file.FullName | ConvertFrom-Json
            Write-Host "File $($file.Name) is readable."
        } catch {
            Write-Host "File $($file.Name) is not readable."
            return $false
        }
    }

    return $true
}


# Main Process
$folderExists = Check-FolderByDate $folderPath

if ($folderExists) {
    $currentDate = (Get-Date).ToString("yyyyMMdd")
    $folderPathFile = "$folderPath/$currentDate"
    $jsonFilesReadable = Check-FolderForJsonFiles -folderPath $folderPathFile

    if ($jsonFilesReadable) {
        Write-Host "All JSON files in $folderPathFile are readable."
    } else {
        Write-Host "Some JSON files in $folderPathFile are not readable or no JSON files found."
        exit 1 # no json file
    }
} else {
    Write-Host "Folder for the current date does not exist."
    exit 1 # no folder
}
