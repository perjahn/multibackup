Set-StrictMode -v latest
$ErrorActionPreference = "Stop"

function Main()
{
    [string] $version = $env:buildversion

    if (!$version)
    {
        Log ("Environment variable buildversion not set!") Red
        exit 1
    }

    Gather-Artifacts $version
}

function Gather-Artifacts([string] $version)
{
    if (Test-Path "C:\Program Files\7-Zip\7z.exe")
    {
        Set-Alias zip "C:\Program Files\7-Zip\7z.exe"
    }
    else
    {
        Set-Alias zip 7zr
    }

    [string] $folder = "backup"

    if (Test-Path $folder)
    {
        Log ("Deleting folder: '" + $folder + "'")
        rd -Recurse $folder -ErrorAction SilentlyContinue
        Start-Sleep 2
    }
    if (Test-Path $folder)
    {
        Log ("Deleting folder (try 2): '" + $folder + "'")
        rd -Recurse $folder -ErrorAction SilentlyContinue
        Start-Sleep 2
    }
    if (Test-Path $folder)
    {
        Log ("Deleting folder (try 3): '" + $folder + "'")
        rd -Recurse $folder
    }


    Log ("Creating folder: '" + $folder + "'")
    md $folder | Out-Null

    copy backupjobs.json,backupschedule.xml $folder
    copy -Recurse (Join-Path "multibackup" "bin" "*" "*" "*" "*") (Join-Path $folder "multibackup")


    [string] $zipfile = "multibackup." + $version + ".7z"

    if (Test-Path $zipfile)
    {
        Log ("Deleting old zipfile: '" + $zipfile + "'")
        del $zipfile
    }

    Log ("Zipping: '" + $folder + "' -> '" + $zipfile + "'")
    zip a -mx9 $zipfile $folder
}

function Log([string] $message, $color)
{
    if ($color)
    {
        Write-Host $message -f $color
    }
    else
    {
        Write-Host $message -f Green
    }
}

Main
