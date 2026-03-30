# =============================================================================
#  Schedule_STO_Pipeline.ps1
#  Creates a Windows Task Scheduler task for the STO Pipeline Orchestrator.
#
#  Usage:
#    .\Schedule_STO_Pipeline.ps1                         # Interactive — prompts for schedule
#    .\Schedule_STO_Pipeline.ps1 -Schedule "Daily14"     # Daily at 14:00
#    .\Schedule_STO_Pipeline.ps1 -Schedule "Hourly"      # Every hour (business hours)
#    .\Schedule_STO_Pipeline.ps1 -Schedule "Custom" -Time "10:30" -Days "Mon,Wed,Fri"
#    .\Schedule_STO_Pipeline.ps1 -Remove                 # Remove the scheduled task
#    .\Schedule_STO_Pipeline.ps1 -RunNow                 # Trigger the task immediately
#
#  Location:
#    \\pl-az-int-prd\D_Drive\Applications\Fusion_Release_4\
#      Fusion_Duracell_EPOS\Fusion_EPOS_Production\STO_Processing\Schedule_STO_Pipeline.ps1
#
# =============================================================================

[CmdletBinding()]
param(
    [ValidateSet("Daily14", "Daily09", "Hourly", "TwiceDaily", "Custom")]
    [string]$Schedule,

    [string]$Time = "14:00",

    [string]$Days = "Mon,Tue,Wed,Thu,Fri",

    [switch]$Remove,

    [switch]$RunNow,

    [switch]$Force,

    [string]$PythonPath = "",

    [string]$Recipients = "aidan.harrington@synoviadigital.com"
)

# =============================================================================
#  CONSTANTS
# =============================================================================

$TaskName        = "Synovia_STO_Pipeline_Orchestrator"
$TaskFolder       = "\Synovia\Fusion_EPOS"
$TaskDescription  = "Runs the STO Pipeline (steps 1-6): CSV ingestion, scheduling, D365 staging, D365 creation, SAP staging, SAP transmission. Sends email report on completion."

$ScriptDir = "\\pl-az-int-prd\D_Drive\Applications\Fusion_Release_4\Fusion_Duracell_EPOS\Fusion_EPOS_Production\STO_Processing"
$OrchestratorScript = Join-Path $ScriptDir "STO_Orchestrator.py"

$LogDir = "\\PL-AZ-FUSION-CO\FusionProduction\Fusion_Hub\Dunnes_EPOS_Inbound\Transfer_Orders_Wasp_Inbound\Logs\Orchestrator"

# =============================================================================
#  HELPER FUNCTIONS
# =============================================================================

function Write-Banner {
    param([string]$Text)
    $line = "=" * 70
    Write-Host ""
    Write-Host $line -ForegroundColor Cyan
    Write-Host "  $Text" -ForegroundColor Cyan
    Write-Host $line -ForegroundColor Cyan
}

function Write-Info {
    param([string]$Text)
    Write-Host "  [INFO]  $Text" -ForegroundColor Gray
}

function Write-OK {
    param([string]$Text)
    Write-Host "  [OK]    $Text" -ForegroundColor Green
}

function Write-Warn {
    param([string]$Text)
    Write-Host "  [WARN]  $Text" -ForegroundColor Yellow
}

function Write-Err {
    param([string]$Text)
    Write-Host "  [ERROR] $Text" -ForegroundColor Red
}

function Find-Python {
    # Try common Python locations
    $candidates = @(
        "C:\Python312\python.exe",
        "C:\Python311\python.exe",
        "C:\Python310\python.exe",
        "C:\Python39\python.exe",
        "C:\Program Files\Python312\python.exe",
        "C:\Program Files\Python311\python.exe",
        "C:\Program Files\Python310\python.exe"
    )

    # Check if python is on PATH
    $pythonOnPath = Get-Command python -ErrorAction SilentlyContinue
    if ($pythonOnPath) {
        return $pythonOnPath.Source
    }

    foreach ($p in $candidates) {
        if (Test-Path $p) {
            return $p
        }
    }

    return $null
}

# =============================================================================
#  REMOVE TASK
# =============================================================================

if ($Remove) {
    Write-Banner "REMOVING SCHEDULED TASK"

    $existing = Get-ScheduledTask -TaskName $TaskName -TaskPath $TaskFolder -ErrorAction SilentlyContinue
    if ($existing) {
        Unregister-ScheduledTask -TaskName $TaskName -TaskPath $TaskFolder -Confirm:(-not $Force)
        Write-OK "Task '$TaskFolder\$TaskName' removed."
    } else {
        Write-Warn "Task '$TaskFolder\$TaskName' not found — nothing to remove."
    }
    exit 0
}

# =============================================================================
#  RUN NOW
# =============================================================================

if ($RunNow) {
    Write-Banner "TRIGGERING IMMEDIATE RUN"

    $existing = Get-ScheduledTask -TaskName $TaskName -TaskPath $TaskFolder -ErrorAction SilentlyContinue
    if ($existing) {
        Start-ScheduledTask -TaskName $TaskName -TaskPath $TaskFolder
        Write-OK "Task triggered. Check logs at:"
        Write-Info $LogDir
    } else {
        Write-Warn "Task not found. Running directly instead..."

        $python = if ($PythonPath) { $PythonPath } else { Find-Python }
        if (-not $python) {
            Write-Err "Python not found. Specify -PythonPath."
            exit 1
        }

        Write-Info "Running: $python `"$OrchestratorScript`" --recipients $Recipients"
        & $python $OrchestratorScript --recipients $Recipients
    }
    exit 0
}

# =============================================================================
#  CREATE / UPDATE SCHEDULED TASK
# =============================================================================

Write-Banner "STO PIPELINE — TASK SCHEDULER SETUP"

# ── Resolve Python ────────────────────────────────────────
$python = if ($PythonPath) { $PythonPath } else { Find-Python }
if (-not $python) {
    Write-Err "Python executable not found."
    Write-Err "Please specify the path with -PythonPath 'C:\Python312\python.exe'"
    exit 1
}
Write-OK "Python: $python"

# ── Verify orchestrator script ────────────────────────────
if (-not (Test-Path $OrchestratorScript)) {
    Write-Err "Orchestrator script not found: $OrchestratorScript"
    exit 1
}
Write-OK "Script: $OrchestratorScript"

# ── Determine schedule ────────────────────────────────────
if (-not $Schedule) {
    Write-Host ""
    Write-Host "  Select a schedule:" -ForegroundColor White
    Write-Host "    1) Daily at 14:00 (recommended)" -ForegroundColor Gray
    Write-Host "    2) Daily at 09:00" -ForegroundColor Gray
    Write-Host "    3) Twice daily (09:00 and 14:00)" -ForegroundColor Gray
    Write-Host "    4) Hourly (business hours 08:00-18:00, Mon-Fri)" -ForegroundColor Gray
    Write-Host "    5) Custom (specify time and days)" -ForegroundColor Gray
    Write-Host ""

    $choice = Read-Host "  Enter choice (1-5)"
    switch ($choice) {
        "1" { $Schedule = "Daily14" }
        "2" { $Schedule = "Daily09" }
        "3" { $Schedule = "TwiceDaily" }
        "4" { $Schedule = "Hourly" }
        "5" { $Schedule = "Custom" }
        default {
            Write-Warn "Invalid choice. Defaulting to Daily14."
            $Schedule = "Daily14"
        }
    }
}

# ── Build trigger(s) ─────────────────────────────────────
$triggers = @()

switch ($Schedule) {
    "Daily14" {
        Write-Info "Schedule: Daily at 14:00 (Mon-Fri)"
        $triggers += New-ScheduledTaskTrigger -Weekly `
            -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday `
            -At "14:00"
    }
    "Daily09" {
        Write-Info "Schedule: Daily at 09:00 (Mon-Fri)"
        $triggers += New-ScheduledTaskTrigger -Weekly `
            -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday `
            -At "09:00"
    }
    "TwiceDaily" {
        Write-Info "Schedule: Twice daily at 09:00 and 14:00 (Mon-Fri)"
        $triggers += New-ScheduledTaskTrigger -Weekly `
            -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday `
            -At "09:00"
        $triggers += New-ScheduledTaskTrigger -Weekly `
            -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday `
            -At "14:00"
    }
    "Hourly" {
        Write-Info "Schedule: Hourly 08:00-18:00 (Mon-Fri)"
        # Task Scheduler doesn't natively do hourly on weekdays,
        # so we create triggers for each hour
        for ($h = 8; $h -le 18; $h++) {
            $timeStr = "{0:D2}:00" -f $h
            $triggers += New-ScheduledTaskTrigger -Weekly `
                -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday `
                -At $timeStr
        }
    }
    "Custom" {
        Write-Info "Schedule: Custom — $Time on $Days"

        # Parse days
        $dayMap = @{
            "Mon" = "Monday"; "Tue" = "Tuesday"; "Wed" = "Wednesday";
            "Thu" = "Thursday"; "Fri" = "Friday"; "Sat" = "Saturday"; "Sun" = "Sunday"
        }
        $dayList = @()
        foreach ($d in ($Days -split ",")) {
            $d = $d.Trim()
            if ($dayMap.ContainsKey($d)) {
                $dayList += $dayMap[$d]
            } elseif ($d -in $dayMap.Values) {
                $dayList += $d
            } else {
                Write-Warn "Unknown day: $d — skipping"
            }
        }

        if ($dayList.Count -eq 0) {
            Write-Err "No valid days specified."
            exit 1
        }

        $triggers += New-ScheduledTaskTrigger -Weekly `
            -DaysOfWeek $dayList `
            -At $Time
    }
}

# ── Build action ──────────────────────────────────────────
$arguments = "`"$OrchestratorScript`" --recipients `"$Recipients`""

$action = New-ScheduledTaskAction `
    -Execute $python `
    -Argument $arguments `
    -WorkingDirectory $ScriptDir

# ── Build settings ────────────────────────────────────────
$settings = New-ScheduledTaskSettingsSet `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -StartWhenAvailable `
    -ExecutionTimeLimit (New-TimeSpan -Hours 2) `
    -RestartCount 2 `
    -RestartInterval (New-TimeSpan -Minutes 5) `
    -MultipleInstances IgnoreNew

# ── Create or update task ─────────────────────────────────
$existing = Get-ScheduledTask -TaskName $TaskName -TaskPath $TaskFolder -ErrorAction SilentlyContinue

if ($existing) {
    Write-Warn "Task already exists — updating..."
    Set-ScheduledTask `
        -TaskName $TaskName `
        -TaskPath $TaskFolder `
        -Action $action `
        -Trigger $triggers `
        -Settings $settings `
        -Description $TaskDescription | Out-Null
    Write-OK "Task updated."
} else {
    Write-Info "Creating new scheduled task..."
    Register-ScheduledTask `
        -TaskName $TaskName `
        -TaskPath $TaskFolder `
        -Action $action `
        -Trigger $triggers `
        -Settings $settings `
        -Description $TaskDescription `
        -RunLevel Highest | Out-Null
    Write-OK "Task created."
}

# ── Summary ───────────────────────────────────────────────
Write-Banner "TASK CONFIGURATION SUMMARY"

Write-Host ""
Write-Host "  Task Name     : $TaskFolder\$TaskName" -ForegroundColor White
Write-Host "  Python        : $python" -ForegroundColor Gray
Write-Host "  Script        : $OrchestratorScript" -ForegroundColor Gray
Write-Host "  Schedule      : $Schedule" -ForegroundColor Gray
Write-Host "  Recipients    : $Recipients" -ForegroundColor Gray
Write-Host "  Log Directory : $LogDir" -ForegroundColor Gray
Write-Host ""
Write-Host "  Useful commands:" -ForegroundColor Yellow
Write-Host "    # Trigger immediately" -ForegroundColor DarkGray
Write-Host "    .\Schedule_STO_Pipeline.ps1 -RunNow" -ForegroundColor Gray
Write-Host ""
Write-Host "    # View task in Task Scheduler" -ForegroundColor DarkGray
Write-Host "    Get-ScheduledTask -TaskName '$TaskName' -TaskPath '$TaskFolder' | Format-List" -ForegroundColor Gray
Write-Host ""
Write-Host "    # Remove the task" -ForegroundColor DarkGray
Write-Host "    .\Schedule_STO_Pipeline.ps1 -Remove" -ForegroundColor Gray
Write-Host ""
Write-Host "    # Run orchestrator manually (with all output)" -ForegroundColor DarkGray
Write-Host "    python `"$OrchestratorScript`" --recipients `"$Recipients`"" -ForegroundColor Gray
Write-Host ""
Write-Host "    # Run report only (resend last report)" -ForegroundColor DarkGray
Write-Host "    python `"$OrchestratorScript`" --report-only --recipients `"$Recipients`"" -ForegroundColor Gray
Write-Host ""
