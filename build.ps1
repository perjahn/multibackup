Set-StrictMode -v latest
$ErrorActionPreference = "Stop"

Set-Alias zip "C:\Program Files\7-Zip\7z.exe"

cd multibackup\bin\*\*\publish

%teamcity.agent.tools.dir%\DllDep\DllDep.exe . "-rSystem.Reflection.TypeExtensions"

zip a -mx9 multibackup.%build.number%.zip *
move *.zip ..\..\..\..\..