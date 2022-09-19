# -s スキーマ名 -t テーブル名 -r 1ループあたりの移行件数
$args = @(
"-s Person -t Person -r 300"
)
$CurrentDir = Split-Path $MyInvocation.MyCommand.Path
$exePath = $CurrentDir + "\DataMigrationTool.exe"

foreach ($arg in $args) {
    Start-Process -FilePath $exePath -ArgumentList $arg -Wait -NoNewWindow
}