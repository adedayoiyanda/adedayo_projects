# Run this script once (as Administrator) to register the daily 2am ETL task.

$taskName   = "CentricApps_ETL_IncrementalLoad"
$pyExe      = "C:\Users\Adedayo\AppData\Local\python-embed\python.exe"
$script     = "C:\Users\Adedayo\Documents\centricapps\bi\etl\pipeline.py"
$workingDir = "C:\Users\Adedayo\Documents\centricapps\bi\etl"
$logDir     = "$workingDir\logs"

# Create logs folder if missing
if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Force $logDir | Out-Null }

$action  = New-ScheduledTaskAction `
    -Execute $pyExe `
    -Argument $script `
    -WorkingDirectory $workingDir

$trigger = New-ScheduledTaskTrigger -Daily -At "04:00AM"

$settings = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit (New-TimeSpan -Hours 4) `
    -RestartCount 2 `
    -RestartInterval (New-TimeSpan -Minutes 30) `
    -StartWhenAvailable `
    -RunOnlyIfNetworkAvailable

# Run as current user; change to a service account if available
$principal = New-ScheduledTaskPrincipal `
    -UserId $env:USERNAME `
    -LogonType S4U `
    -RunLevel Highest

# Remove existing task with same name if present
Unregister-ScheduledTask -TaskName $taskName -Confirm:$false -ErrorAction SilentlyContinue

Register-ScheduledTask `
    -TaskName  $taskName `
    -Action    $action `
    -Trigger   $trigger `
    -Settings  $settings `
    -Principal $principal `
    -Description "Daily incremental load: MariaDB abia_central -> PostgreSQL (4am)"

Write-Output ""
Write-Output "Task registered: $taskName"
Write-Output "Schedule: daily at 4:00 AM"
Write-Output "Log files: $logDir"
Write-Output ""
Write-Output "To run it immediately for testing:"
Write-Output "  Start-ScheduledTask -TaskName '$taskName'"
Write-Output ""
Write-Output "To test the pipeline manually (ETL + dbt + email):"
Write-Output "  & '$pyExe' '$script'"
Write-Output ""
Write-Output "To check last run status:"
Write-Output "  Get-ScheduledTaskInfo -TaskName '$taskName' | Select-Object LastRunTime, LastTaskResult"
