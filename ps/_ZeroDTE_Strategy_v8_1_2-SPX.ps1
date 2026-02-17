<#
_File: _ZeroDTE_Strategy_v8_1_2.ps1  (VERSION: V8.1.2)
PREVIOUS VERSION: V8.1.1   (revert file: _ZeroDTE_Strategy_v8_1_1.ps1)

V8.1.2 CHANGES
- FIX: Cursor LastWriteUtc reconstruction (was incorrectly using FromFileTimeUtc on DateTime ticks)
- IMPROVE: Key-level option mid lookup now supports tolerant strike matching + nearest-strike fallback
  - Helps when strikes are floating / not perfectly equal in JSON
- No payload schema change vs 8.1.1 (still 28 fields); header bumped to 8.1.2 for traceability

NOTES
- PowerShell 7+
#>

[CmdletBinding()]
param(
  [string]$Symbol = "SPX",

  # Input snapshots folder
  [string]$RootPath = "",

  # Optional: where EOD snapshots live (defaults to RootPath\_Historical)
  [string]$HistoricalPath = "",

  # include _Historical EOD snapshots for scanning
  [switch]$IncludeHistorical,

  # ± band around spot for analysis universe
  [double]$BandPct = 0.05,

  [int]$TopN = 15,

  # Output folder (defaults to C:\Users\Nathan\Documents\MarketData\<SYMBOL>_ZeroDTE)
  [string]$OutDir = "",

  [int]$ContractMultiplier = 100,

  # most-recent trading dates to include (drives which dates we emit payloads for)
  [int]$LookbackDays = 20,

  [int]$SnapshotIntervalMinutes = 15,

  # recalc levels per RTH slot within day (used for analysis/caching)
  [switch]$HourlySnapshots = $true,

  [switch]$ShowKeyDebug,

  # extra verbatim output
  [bool]$VerboseOutput = $true,

  # ignore cache and recompute everything
  [switch]$ForceReprocess,

  # -------------------- Effective observed timestamp --------------------
  [switch]$UseFileWriteTimeAsObserved,

  [ValidateSet("WriteTimeUtc","FileName")]
  [string]$ObservedTimestampSource = "WriteTimeUtc",

  # -------------------- Per-file Checkpoint (sig based) --------------------
  [string]$CheckpointPath = "",

  [switch]$UseCheckpointFastMode,

  # -------------------- Cursor checkpoint (date/ticks based) --------------------
  [switch]$UseCheckpointCursor,

  [string]$CheckpointCursorPath = "",

  # -------------------- Payload output (legacy single file) --------------------
  [string]$PayloadOutPath = "",

  [switch]$CopyPayloadToClipboard,

  # TradingView input string safety cap (trim oldest segments if exceeded)
  [int]$MaxPayloadChars = 9000,

  # Additional safety cap on number of segments embedded into the payload
  [int]$MaxSegments = 450,

  # ==================== Per-day payload emission ====================
  # If set, only emit payload(s) for this observed date (accepts yyyy-MM-dd or yyyyMMdd)
  [string]$OnlyPayloadDate = "",

  # Where per-day payloads are written (defaults to: OutDir\Payloads_V8_1_2)
  [string]$PayloadOutDir = "",

  # Emit per-day payloads (default ON)
  [switch]$EmitPerDayPayloads,

  # Emit Hourly payloads (H) using SnapshotIntervalMinutes slots (default ON)
  [switch]$EmitHourlyPayloads,

  # Emit Daily payloads (D) one segment per day (default ON)
  [switch]$EmitDailyPayloads,

  # Which payload kinds to emit
  [ValidateSet("0DTE","WEEKLY","BOTH")]
  [string]$EmitPayloadKinds = "BOTH",

  # Include weekly expiration date in weekly payload filenames (default ON)
  [switch]$IncludeWeeklyExpirationInFileName,

  # -------------------- NEW (V8.1.2): strike matching tolerance --------------------
  # Used for key-level option mid lookups when strike equality is not exact
  [double]$StrikeMatchTolerance = 0.01
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

# -------------------- Defaults driven by Symbol --------------------
function Normalize-Symbol {
  param([string]$s)
  if ([string]::IsNullOrWhiteSpace($s)) { return "SPX" }
  return ($s.Trim().ToUpperInvariant())
}
$Symbol = Normalize-Symbol $Symbol

if ([string]::IsNullOrWhiteSpace($RootPath)) {
  $RootPath = Join-Path "C:\Users\Nathan\Documents\MarketData" $Symbol
}
if ([string]::IsNullOrWhiteSpace($OutDir)) {
  $OutDir = Join-Path "C:\Users\Nathan\Documents\MarketData" ("{0}_ZeroDTE" -f $Symbol)
}
if (-not (Test-Path $OutDir)) { New-Item -ItemType Directory -Path $OutDir | Out-Null }

if ([string]::IsNullOrWhiteSpace($CheckpointPath)) {
  $CheckpointPath = Join-Path $OutDir "_ZeroDTE_Checkpoint_V8_1_2.json"
}
if ([string]::IsNullOrWhiteSpace($CheckpointCursorPath)) {
  $CheckpointCursorPath = Join-Path $OutDir "_ZeroDTE_CheckpointCursor_V8_1_2.json"
}
if ([string]::IsNullOrWhiteSpace($PayloadOutPath)) {
  # Legacy combined payload path (still written below only if you opt to copy to clipboard; otherwise per-day files are primary)
  $PayloadOutPath = Join-Path $OutDir ("Payload_V8_1_2_{0}.txt" -f $Symbol)
}
if ([string]::IsNullOrWhiteSpace($PayloadOutDir)) {
  $PayloadOutDir = Join-Path $OutDir "Payloads_V8_1_2"
}
if (-not (Test-Path $PayloadOutDir)) { New-Item -ItemType Directory -Path $PayloadOutDir | Out-Null }

# If user didn't specify, default cursor ON
if (-not $PSBoundParameters.ContainsKey('UseCheckpointCursor')) {
  $UseCheckpointCursor = $true
}

# If user didn't specify, default clipboard copy OFF (safe)
if (-not $PSBoundParameters.ContainsKey('CopyPayloadToClipboard')) {
  $CopyPayloadToClipboard = $false
}

# New switches default ON if not provided
if (-not $PSBoundParameters.ContainsKey('EmitPerDayPayloads')) { $EmitPerDayPayloads = $true }
if (-not $PSBoundParameters.ContainsKey('EmitHourlyPayloads')) { $EmitHourlyPayloads = $true }
if (-not $PSBoundParameters.ContainsKey('EmitDailyPayloads'))  { $EmitDailyPayloads  = $true }
if (-not $PSBoundParameters.ContainsKey('IncludeWeeklyExpirationInFileName')) { $IncludeWeeklyExpirationInFileName = $true }

# -------------------- Helpers --------------------
function To-KInt {
  param($x)
  try {
    if ($null -eq $x) { return 0 }
    $d = [double]$x
    return [int][math]::Round($d * 1000.0, 0)  # "K" scale
  } catch { return 0 }
}
function Get-DirectionalMidAtStrike {
  param(
    $Rows,
    [double]$Strike,
    [double]$Spot,
    [double]$Tol = 0.01
  )

  if ($null -eq $Rows -or $null -eq $Strike -or $Spot -le 0) { return $null }

  # OTM-side selection relative to spot:
  # - strike above spot => CALL is OTM
  # - strike below spot => PUT is OTM
  $side = if ([double]$Strike -ge [double]$Spot) { "call" } else { "put" }

  return (Get-OptionMidAtStrikeSide -Rows $Rows -Strike $Strike -Side $side -Tol $Tol)
}

function Normalize-Side {
  param([string]$Side)
  if ([string]::IsNullOrWhiteSpace($Side)) { return $null }
  $s = $Side.Trim().ToLowerInvariant()
  switch ($s) {
    "c"      { "call" }
    "call"   { "call" }
    "calls"  { "call" }
    "p"      { "put" }
    "put"    { "put" }
    "puts"   { "put" }
    default  { $s }   # keep as-is for debugging
  }
}

function Parse-SnapshotFileName {
  param([string]$FileName)

  $pattern = '^(?<Ticker>[A-Z]+)-(?<Spot>\d+(\.\d+)?)-(?<ExpYear>\d{4})-(?<ExpMonth>\d{2})-(?<ExpDay>\d{2})-(?<ObsDate>\d{8})-(?<ObsTime>\d{6})\.json$'
  if ($FileName -notmatch $pattern) { return $null }

  $exp   = Get-Date -Year $Matches.ExpYear -Month $Matches.ExpMonth -Day $Matches.ExpDay -Hour 0 -Minute 0 -Second 0
  $obsDT = [datetime]::ParseExact(($Matches.ObsDate + $Matches.ObsTime), 'yyyyMMddHHmmss', $null)

  [pscustomobject]@{
    Ticker       = $Matches.Ticker
    SpotInName   = [double]$Matches.Spot
    Expiration   = $exp.Date
    ObservedDT   = $obsDT
    ObservedDate = $obsDT.Date
  }
}

function Get-ArrayOrNull {
  param($Json, [string]$Key)
  if ($null -eq $Json) { return $null }

  if ($Json.PSObject -and ($Json.PSObject.Properties.Name -contains $Key)) {
    return $Json.$Key
  }
  if ($Json -is [System.Collections.IDictionary]) {
    if ($Json.Contains($Key)) { return $Json[$Key] }
  }
  return $null
}

function To-Num {
  param($v)
  if ($null -eq $v) { return $null }
  if ($v -is [string] -and [string]::IsNullOrWhiteSpace($v)) { return $null }
  try { return [double]$v } catch { return $null }
}

function Get-OptionMidFromCols {
  param(
    $Mid,
    $Bid,
    $Ask
  )

  $m = To-Num $Mid
  if ($null -ne $m -and $m -gt 0) { return [double]$m }

  $b = To-Num $Bid
  $a = To-Num $Ask
  if ($null -ne $b -and $null -ne $a -and $b -gt 0 -and $a -gt 0) {
    return [double](($b + $a) / 2.0)
  }

  return $null
}

function Get-StrikeMatches {
  param(
    $Rows,
    [double]$Strike,
    [double]$Tol
  )

  if ($null -eq $Rows -or $null -eq $Strike) { return @() }
  if ($Tol -lt 0) { $Tol = 0 }

  $s0 = [double]$Strike
  $hits = @(
    $Rows | Where-Object {
      $_.strike -ne $null -and ([math]::Abs([double]$_.strike - $s0) -le $Tol)
    }
  )

  return $hits
}

function Get-OptionMidAtStrikeSide {
  param(
    $Rows,
    [double]$Strike,
    [ValidateSet("call","put")]
    [string]$Side,
    [double]$Tol = 0.01
  )

  if ($null -eq $Rows -or $null -eq $Strike) { return $null }

  $cands = @(Get-StrikeMatches -Rows $Rows -Strike $Strike -Tol $Tol | Where-Object { $_.side -eq $Side })
  $mids = @($cands | Where-Object { $_.mid -ne $null -and [double]$_.mid -gt 0 } | Select-Object -ExpandProperty mid)

  if ($mids.Count -gt 0) {
    return [double](($mids | Measure-Object -Average).Average)
  }

  # Fallback: nearest strike (same side) with a valid mid
  $nearest = @(
    $Rows |
    Where-Object { $_.strike -ne $null -and $_.side -eq $Side -and $_.mid -ne $null -and [double]$_.mid -gt 0 } |
    Sort-Object { [math]::Abs([double]$_.strike - [double]$Strike) } |
    Select-Object -First 1
  )

  if ($nearest.Count -gt 0 -and $null -ne $nearest[0]) {
    return [double]$nearest[0].mid
  }

  return $null
}

function Get-StraddleMidAtStrike {
  param(
    $Rows,
    [double]$Strike,
    [double]$Tol = 0.01
  )

  if ($null -eq $Strike) { return $null }

  $c = Get-OptionMidAtStrikeSide -Rows $Rows -Strike $Strike -Side "call" -Tol $Tol
  $p = Get-OptionMidAtStrikeSide -Rows $Rows -Strike $Strike -Side "put"  -Tol $Tol

  if ($null -eq $c -and $null -eq $p) { return $null }
  if ($null -eq $c) { return [double]$p }
  if ($null -eq $p) { return [double]$c }

  return [double]($c + $p)
}

function Get-ValueAt {
  param($Arr, [int]$Index)
  if ($null -eq $Arr) { return $null }
  if ($Arr -isnot [System.Collections.IList]) { return $Arr }
  if ($Index -lt 0 -or $Index -ge $Arr.Count) { return $null }
  return $Arr[$Index]
}

function Build-RowsFromColumnarJson {
  param($Json)

  $symbolsRaw = Get-ArrayOrNull $Json "optionSymbol"
  if ($null -eq $symbolsRaw) { throw "JSON missing optionSymbol[] (cannot align rows)." }
  $symbols = @($symbolsRaw)

  $n = $symbols.Count

  $strike = @(Get-ArrayOrNull $Json "strike")
  $side   = @(Get-ArrayOrNull $Json "side")
  $oi     = @(Get-ArrayOrNull $Json "openInterest")
  $vol    = @(Get-ArrayOrNull $Json "volume")
  $uPx    = @(Get-ArrayOrNull $Json "underlyingPrice")
  $gamma  = @(Get-ArrayOrNull $Json "gamma")
  $iv     = @(Get-ArrayOrNull $Json "iv")
  $vega   = @(Get-ArrayOrNull $Json "vega")

  # option mid price (supports multiple key names)
  $midRaw = Get-ArrayOrNull $Json "mid"
  if ($null -eq $midRaw) { $midRaw = Get-ArrayOrNull $Json "midPrice" }
  if ($null -eq $midRaw) { $midRaw = Get-ArrayOrNull $Json "mark" }
  $mid = @($midRaw)

  # Optional fallback inputs (only used if mid is missing)
  $bid = @(Get-ArrayOrNull $Json "bid")
  $ask = @(Get-ArrayOrNull $Json "ask")

  $rows = New-Object System.Collections.Generic.List[object]
  for ($i = 0; $i -lt $n; $i++) {
    $rows.Add([pscustomobject]@{
      optionSymbol    = Get-ValueAt $symbols $i
      strike          = To-Num (Get-ValueAt $strike $i)
      side            = Normalize-Side ("" + (Get-ValueAt $side $i))   # normalize here
      openInterest    = To-Num (Get-ValueAt $oi $i)
      volume          = To-Num (Get-ValueAt $vol $i)
      underlyingPrice = To-Num (Get-ValueAt $uPx $i)
      gamma           = To-Num (Get-ValueAt $gamma $i)
      iv              = To-Num (Get-ValueAt $iv $i)
      vega            = To-Num (Get-ValueAt $vega $i)

      mid             = (Get-OptionMidFromCols -Mid (Get-ValueAt $mid $i) -Bid (Get-ValueAt $bid $i) -Ask (Get-ValueAt $ask $i))
    }) | Out-Null
  }
  return $rows
}

function Convert-ToEastern {
  param([datetime]$Dt)

  $tzET = [System.TimeZoneInfo]::FindSystemTimeZoneById("Eastern Standard Time")
  $tzLocal = [System.TimeZoneInfo]::Local

  if ($Dt.Kind -eq [System.DateTimeKind]::Utc) {
    return [System.TimeZoneInfo]::ConvertTimeFromUtc($Dt, $tzET)
  }
  if ($Dt.Kind -eq [System.DateTimeKind]::Local) {
    return [System.TimeZoneInfo]::ConvertTime($Dt, $tzLocal, $tzET)
  }

  $assumedLocal = [datetime]::SpecifyKind($Dt, [System.DateTimeKind]::Local)
  return [System.TimeZoneInfo]::ConvertTime($assumedLocal, $tzLocal, $tzET)
}

function Sum-Prop {
  param($Items, [string]$Prop)
  if ($null -eq $Items) { return 0.0 }
  $m = $Items | Measure-Object -Property $Prop -Sum
  if ($null -eq $m -or $null -eq $m.Sum) { return 0.0 }
  return [double]$m.Sum
}

function Get-RthSlotsForDateET {
  param(
    [datetime]$DateET,
    [int]$IntervalMinutes = 60
  )

  if ($IntervalMinutes -lt 1) { $IntervalMinutes = 1 }

  $d = $DateET.Date
  $slots = New-Object System.Collections.Generic.List[object]

  $start = Get-Date -Year $d.Year -Month $d.Month -Day $d.Day -Hour 9 -Minute 30 -Second 0
  $end   = Get-Date -Year $d.Year -Month $d.Month -Day $d.Day -Hour 16 -Minute 0 -Second 0

  $cur = $start
  $idx = 0
  while ($cur -lt $end) {
    $nxt = $cur.AddMinutes($IntervalMinutes)
    if ($nxt -gt $end) { $nxt = $end }
    $slots.Add([pscustomobject]@{ SlotIndex=$idx; StartET=$cur; EndET=$nxt }) | Out-Null
    $cur = $nxt
    $idx++
  }
  return $slots
}

function Get-RthSlotIndex {
  param(
    [datetime]$ObservedET,
    [int]$IntervalMinutes = 60
  )

  $slots = Get-RthSlotsForDateET -DateET $ObservedET.Date -IntervalMinutes $IntervalMinutes
  foreach ($s in $slots) {
    if ($ObservedET -ge $s.StartET -and $ObservedET -lt $s.EndET) { return [int]$s.SlotIndex }
  }
  return $null
}

function Get-WeeklyExpiration {
  param([datetime]$Date)
  $d = $Date.Date
  $dow = [int]$d.DayOfWeek
  $fri = 5
  $delta = $fri - $dow
  if ($delta -lt 0) { $delta += 7 }
  return $d.AddDays($delta)
}

function Get-ThirdFriday {
  param([int]$Year, [int]$Month)
  $first = Get-Date -Year $Year -Month $Month -Day 1 -Hour 0 -Minute 0 -Second 0
  $dow = [int]$first.DayOfWeek
  $fri = 5
  $delta = $fri - $dow
  if ($delta -lt 0) { $delta += 7 }
  $firstFriday = $first.AddDays($delta)
  return $firstFriday.AddDays(14)
}

function Get-MonthlyExpiration {
  param([datetime]$Date)
  $d = $Date.Date
  $thirdFriThis = Get-ThirdFriday -Year $d.Year -Month $d.Month
  if ($d -le $thirdFriThis) { return $thirdFriThis }
  $nm = $d.AddMonths(1)
  return (Get-ThirdFriday -Year $nm.Year -Month $nm.Month)
}

# -------------------- Strength scoring --------------------
function Clamp-Num {
  param([double]$x, [double]$lo, [double]$hi)
  if ($x -lt $lo) { return $lo }
  if ($x -gt $hi) { return $hi }
  return $x
}

function Compute-MagnetScore {
  param(
    [double]$CurAbsGex,
    [double]$PrevAbsGex
  )

  if ($CurAbsGex -le 0) { return 0 }
  if ($PrevAbsGex -le 0) { return 50 }

  $ratio = $CurAbsGex / $PrevAbsGex

  $lr = 0.0
  try { $lr = [math]::Log10($ratio) } catch { $lr = 0.0 }

  $score = 50.0 + (25.0 * $lr)
  $score = Clamp-Num -x $score -lo 0 -hi 100
  return [int][math]::Round($score, 0)
}

# -------------------- IV helpers --------------------
function Normalize-IV {
  param([double]$iv)

  if ($iv -le 0) { return $null }

  if ($iv -gt 5.0) { $iv = $iv / 100.0 }
  if ($iv -lt 0.0001) { return $null }
  if ($iv -gt 5.0) { $iv = 5.0 }

  return [double]$iv
}

function Get-AtmCallPutIV {
  param(
    $Rows,
    [double]$Spot
  )

  if ($null -eq $Rows -or $Spot -le 0) {
    return [pscustomobject]@{ CallIV=$null; PutIV=$null; MidIV=$null; AtmStrike=$null }
  }

  $cand = @($Rows | Where-Object { $_.strike -ne $null -and $_.iv -ne $null -and [double]$_.iv -gt 0 })
  if ($cand.Count -eq 0) {
    return [pscustomobject]@{ CallIV=$null; PutIV=$null; MidIV=$null; AtmStrike=$null }
  }

  $strikes = @($cand | Select-Object -ExpandProperty strike -Unique)
  if ($strikes.Count -eq 0) {
    return [pscustomobject]@{ CallIV=$null; PutIV=$null; MidIV=$null; AtmStrike=$null }
  }

  $nearest = $strikes | Sort-Object { [math]::Abs([double]$_ - $Spot) } | Select-Object -First 1
  if ($null -eq $nearest) {
    return [pscustomobject]@{ CallIV=$null; PutIV=$null; MidIV=$null; AtmStrike=$null }
  }

  $atStrike = @($cand | Where-Object { [double]$_.strike -eq [double]$nearest })

  $callIVs = @($atStrike | Where-Object { $_.side -eq "call" -and [double]$_.iv -gt 0 } | Select-Object -ExpandProperty iv)
  $putIVs  = @($atStrike | Where-Object { $_.side -eq "put"  -and [double]$_.iv -gt 0 } | Select-Object -ExpandProperty iv)

  $callIV = $null
  $putIV  = $null

  if ($callIVs.Count -gt 0) {
    $callIV = Normalize-IV -iv ([double](($callIVs | Measure-Object -Average).Average))
  }
  if ($putIVs.Count -gt 0) {
    $putIV = Normalize-IV -iv ([double](($putIVs | Measure-Object -Average).Average))
  }

  $mid = $null
  if ($null -ne $callIV -and $null -ne $putIV) { $mid = [double](($callIV + $putIV) / 2.0) }
  elseif ($null -ne $callIV) { $mid = [double]$callIV }
  elseif ($null -ne $putIV)  { $mid = [double]$putIV }

  return [pscustomobject]@{
    CallIV    = $callIV
    PutIV     = $putIV
    MidIV     = $mid
    AtmStrike = [double]$nearest
  }
}

function Compute-IVBand {
  param(
    [double]$Spot,
    [double]$AtmIV,
    [datetime]$ObservedET,
    [datetime]$ExpirationDate
  )

  if ($Spot -le 0 -or $null -eq $AtmIV -or $AtmIV -le 0) {
    return [pscustomobject]@{ Upper=$null; Lower=$null; IV=$AtmIV; TYears=0.0; Move=$null }
  }

  $expET = Get-Date -Year $ExpirationDate.Year -Month $ExpirationDate.Month -Day $ExpirationDate.Day -Hour 16 -Minute 0 -Second 0

  $secs = 0.0
  try { $secs = ($expET - $ObservedET).TotalSeconds } catch { $secs = 0.0 }
  if ($secs -lt 0) { $secs = 0.0 }

  $tYears = $secs / (365.0 * 24.0 * 3600.0)

  $move = $null
  $upper = $null
  $lower = $null

  try {
    $move = $Spot * $AtmIV * [math]::Sqrt([math]::Max(0.0, $tYears))
    $upper = $Spot + $move
    $lower = $Spot - $move
  } catch {
    $move = $null
    $upper = $null
    $lower = $null
  }

  return [pscustomobject]@{
    Upper=$upper
    Lower=$lower
    IV=$AtmIV
    TYears=$tYears
    Move=$move
  }
}

function Ensure-LevelProps {
  param($lvl)

  if ($null -eq $lvl) { return $null }

  $need = @(
    "IVUpper","IVLower","IVMove","IVTYears",
    "AtmIVCall","AtmIVPut","AtmIVMid","AtmIVStrike",
    "CallWallAbsGEX","PutWallAbsGEX","MagnetAbsGEX","VegaNet","VegaAbs",
    "CallWallMid","PutWallMid","MagnetMid","FlipMid"
  )

  foreach ($n in $need) {
    if (-not ($lvl.PSObject.Properties.Name -contains $n)) {
      $lvl | Add-Member -NotePropertyName $n -NotePropertyValue $null -Force
    }
  }

  return $lvl
}

# -------------------- Compute levels from a JSON file (snapshot) --------------------
function Compute-LevelsFromFile {
  param(
    [string]$FullName,
    [pscustomobject]$Meta,
    [int]$ContractMultiplier,
    [double]$BandPct,
    [datetime]$ObservedET,
    [double]$StrikeTol
  )

  $json = Get-Content -Raw -Path $FullName | ConvertFrom-Json
  $rows = Build-RowsFromColumnarJson -Json $json

  # Prefer filename spot, fallback to JSON
  $spot = $Meta.SpotInName
  if ($null -eq $spot -or $spot -le 0) {
    $spot = ($rows | Select-Object -First 1).underlyingPrice
  }

  $lo = $spot * (1.0 - $BandPct)
  $hi = $spot * (1.0 + $BandPct)

  $band = $rows | Where-Object {
    ($_.strike -ne $null) -and ($_.strike -ge $lo) -and ($_.strike -le $hi)
  }

  $calls = $band | Where-Object { $_.side -eq "call" }
  $puts  = $band | Where-Object { $_.side -eq "put"  }

  # -------------------- VEGA --------------------
  $vegaNet = 0.0
  $vegaAbs = 0.0

  foreach ($r in $band) {
    if ($null -eq $r.vega -or $null -eq $r.openInterest) { continue }
    $v = [double]$r.vega
    if ($v -eq 0) { continue }

    $oi2 = [double]$r.openInterest
    $signed = if ($r.side -eq "put") { -1.0 } else { 1.0 }

    $contrib = $v * $oi2 * $ContractMultiplier
    $vegaNet += ($contrib * $signed)
    $vegaAbs += [math]::Abs($contrib)
  }

  $callVol = Sum-Prop $calls "volume"
  $putVol  = Sum-Prop $puts  "volume"
  $callOI  = Sum-Prop $calls "openInterest"
  $putOI   = Sum-Prop $puts  "openInterest"

  $pressure = $null
  $den = ($callVol + $putVol)
  if ($den -gt 0) { $pressure = [math]::Round((($callVol - $putVol) / $den), 4) }

  $spot2 = $spot * $spot
  $byStrike =
    $band |
    Where-Object { $_.strike -ne $null } |
    Group-Object strike |
    ForEach-Object {
      $g = $_.Group

      $totVol = Sum-Prop $g "volume"
      $totOI  = Sum-Prop $g "openInterest"

      $netGex = 0.0
      foreach ($r in $g) {
        if ($null -eq $r.gamma -or $null -eq $r.openInterest) { continue }
        $sign = if ($r.side -eq "put") { -1.0 } else { 1.0 }
        $netGex += ($r.gamma * $r.openInterest * $ContractMultiplier * $spot2 * $sign)
      }

      [pscustomobject]@{
        Strike   = [double]$_.Name
        TotalVol = [int][math]::Round($totVol,0)
        TotalOI  = [int][math]::Round($totOI,0)
        NetGEX   = [double]$netGex
        AbsGEX   = [double]([math]::Abs($netGex))
      }
    } |
    Sort-Object Strike

  $callWall = $null
  $putWall  = $null
  $magnet   = $null
  $flip     = $null

  $callWallAbsGEX = 0.0
  $callWallNetGEX = 0.0
  $putWallAbsGEX  = 0.0
  $putWallNetGEX  = 0.0
  $magnetAbsGEX   = 0.0
  $magnetNetGEX   = 0.0

  $bsCount = 0
  try { $bsCount = ($byStrike | Measure-Object).Count } catch { $bsCount = 0 }

  if ($bsCount -gt 0) {
    $maxPos = $byStrike | Sort-Object NetGEX -Descending | Select-Object -First 1
    if ($null -ne $maxPos) {
      $callWall = [double]$maxPos.Strike
      $callWallAbsGEX = [double]$maxPos.AbsGEX
      $callWallNetGEX = [double]$maxPos.NetGEX
    }

    $maxNeg = $byStrike | Sort-Object NetGEX | Select-Object -First 1
    if ($null -ne $maxNeg) {
      $putWall  = [double]$maxNeg.Strike
      $putWallAbsGEX = [double]$maxNeg.AbsGEX
      $putWallNetGEX = [double]$maxNeg.NetGEX
    }

    $magLo = $spot * 0.99
    $magHi = $spot * 1.01
    $magRow = @(
      $byStrike |
      Where-Object { $_.Strike -ge $magLo -and $_.Strike -le $magHi } |
      Sort-Object AbsGEX -Descending |
      Select-Object -First 1
    )
    if ($magRow.Count -gt 0 -and $null -ne $magRow[0]) {
      $magnet = [double]$magRow[0].Strike
      $magnetAbsGEX = [double]$magRow[0].AbsGEX
      $magnetNetGEX = [double]$magRow[0].NetGEX
    }

    $cum = 0.0
    $prevCum = $null
    $prevStrike = $null
    foreach ($s in $byStrike) {
      $cum += $s.NetGEX
      if ($null -ne $prevCum) {
        if (($prevCum -lt 0 -and $cum -ge 0) -or ($prevCum -gt 0 -and $cum -le 0)) {
          $flip = if ([math]::Abs($prevCum) -le [math]::Abs($cum)) { [double]$prevStrike } else { [double]$s.Strike }
          break
        }
      }
      $prevCum = $cum
      $prevStrike = $s.Strike
    }
  }

  # -------------------- key-level option mids --------------------
  $callWallMid = $null
  $putWallMid  = $null
  $magnetMid   = $null
  $flipMid     = $null

    # Directional (OTM-side) mids relative to spot
    if ($null -ne $callWall) { $callWallMid = Get-DirectionalMidAtStrike -Rows $rows -Strike $callWall -Spot $spot -Tol $StrikeTol }
    if ($null -ne $putWall)  { $putWallMid  = Get-DirectionalMidAtStrike -Rows $rows -Strike $putWall  -Spot $spot -Tol $StrikeTol }
    if ($null -ne $magnet)   { $magnetMid   = Get-DirectionalMidAtStrike -Rows $rows -Strike $magnet   -Spot $spot -Tol $StrikeTol }
    if ($null -ne $flip)     { $flipMid     = Get-DirectionalMidAtStrike -Rows $rows -Strike $flip     -Spot $spot -Tol $StrikeTol }


  # IV
  $atmObj = Get-AtmCallPutIV -Rows $rows -Spot $spot
  $atmIVMid  = $atmObj.MidIV
  $ivBand = Compute-IVBand -Spot $spot -AtmIV $atmIVMid -ObservedET $ObservedET -ExpirationDate $Meta.Expiration

  [pscustomobject]@{
    Spot         = [math]::Round($spot, 2)
    CallWall     = $callWall
    PutWall      = $putWall
    Magnet       = $magnet
    Flip         = $flip
    Pressure     = $pressure
    ByStrike     = $byStrike
    CallVol      = [int][math]::Round($callVol,0)
    PutVol       = [int][math]::Round($putVol,0)
    CallOI       = [int][math]::Round($callOI,0)
    PutOI        = [int][math]::Round($putOI,0)
    StrikeLo     = [math]::Round($lo, 2)
    StrikeHi     = [math]::Round($hi, 2)

    CallWallAbsGEX = $callWallAbsGEX
    CallWallNetGEX = $callWallNetGEX
    PutWallAbsGEX  = $putWallAbsGEX
    PutWallNetGEX  = $putWallNetGEX
    MagnetAbsGEX   = $magnetAbsGEX
    MagnetNetGEX   = $magnetNetGEX
    VegaNet        = $vegaNet
    VegaAbs        = $vegaAbs

    CallWallMid    = $callWallMid
    PutWallMid     = $putWallMid
    MagnetMid      = $magnetMid
    FlipMid        = $flipMid

    AtmIVCall    = $atmObj.CallIV
    AtmIVPut     = $atmObj.PutIV
    AtmIVMid     = $atmObj.MidIV
    AtmIVStrike  = $atmObj.AtmStrike

    IVUpper      = $ivBand.Upper
    IVLower      = $ivBand.Lower
    IVMove       = $ivBand.Move
    IVTYears     = $ivBand.TYears
  }
}

# -------------------- Cache helpers --------------------
function Load-Cache {
  param([string]$Path)
  if (-not (Test-Path $Path)) {
    return [pscustomobject]@{ Version="V8.1.2"; Created=(Get-Date).ToString("s"); Entries=@{} }
  }
  try {
    $raw = Get-Content -Raw -Path $Path
    if ([string]::IsNullOrWhiteSpace($raw)) {
      return [pscustomobject]@{ Version="V8.1.2"; Created=(Get-Date).ToString("s"); Entries=@{} }
    }

    $obj = $raw | ConvertFrom-Json -Depth 60
    if ($null -eq $obj) {
      return [pscustomobject]@{ Version="V8.1.2"; Created=(Get-Date).ToString("s"); Entries=@{} }
    }

    if ($null -eq $obj.Entries) {
      $obj | Add-Member -NotePropertyName Entries -NotePropertyValue @{} -Force
    }

    if ($obj.Entries -isnot [hashtable]) {
      $ht = @{}
      foreach ($p in $obj.Entries.PSObject.Properties) {
        $ht[$p.Name] = $p.Value
      }
      $obj.Entries = $ht
    }

    return $obj
  } catch {
    throw "Failed to load cache at $Path : $($_.Exception.Message)"
  }
}

function Save-Cache {
  param($Cache, [string]$Path)
  $payload = [pscustomobject]@{
    Version = "V8.1.2"
    Updated = (Get-Date).ToString("s")
    Entries = $Cache.Entries
  }
  $json = $payload | ConvertTo-Json -Depth 60
  Set-Content -Path $Path -Value $json -Encoding UTF8
}

function Get-FileSig {
  param([System.IO.FileInfo]$File)
  [pscustomobject]@{
    LastWriteUtcTicks = $File.LastWriteTimeUtc.Ticks
    Length            = $File.Length
  }
}

function Cache-Hit {
  param($Cache, [string]$Key, $Sig, [switch]$ForceReprocess)
  if ($ForceReprocess) { return $false }
  if ($null -eq $Cache -or $null -eq $Cache.Entries) { return $false }
  if (-not $Cache.Entries.ContainsKey($Key)) { return $false }
  $e = $Cache.Entries[$Key]
  if ($null -eq $e) { return $false }
  return ($e.LastWriteUtcTicks -eq $Sig.LastWriteUtcTicks -and $e.Length -eq $Sig.Length)
}

# -------------------- Per-file Checkpoint (sig-based) --------------------
function Load-Checkpoint {
  param([string]$Path)

  if (-not (Test-Path $Path)) {
    return [pscustomobject]@{
      Version = "V8.1.2"
      UpdatedUtc = (Get-Date).ToUniversalTime().ToString("s")
      Files = @{}
    }
  }

  try {
    $raw = Get-Content -Raw -Path $Path
    if ([string]::IsNullOrWhiteSpace($raw)) { throw "Empty checkpoint" }

    $obj = $raw | ConvertFrom-Json -Depth 50
    if ($null -eq $obj.Files) {
      $obj | Add-Member -NotePropertyName Files -NotePropertyValue @{} -Force
    }

    if ($obj.Files -isnot [hashtable]) {
      $ht = @{}
      foreach ($p in $obj.Files.PSObject.Properties) { $ht[$p.Name] = $p.Value }
      $obj.Files = $ht
    }

    return $obj
  } catch {
    Write-Warning "Checkpoint load failed ($Path): $($_.Exception.Message). Starting fresh."
    return [pscustomobject]@{
      Version = "V8.1.2"
      UpdatedUtc = (Get-Date).ToUniversalTime().ToString("s")
      Files = @{}
    }
  }
}

function Save-Checkpoint {
  param($Checkpoint, [string]$Path)

  $payload = [pscustomobject]@{
    Version = "V8.1.2"
    UpdatedUtc = (Get-Date).ToUniversalTime().ToString("s")
    Files = $Checkpoint.Files
  }

  $json = $payload | ConvertTo-Json -Depth 50
  Set-Content -Path $Path -Value $json -Encoding UTF8
}

function Checkpoint-Hit {
  param(
    $Checkpoint,
    [string]$FullName,
    $Sig
  )

  if ($null -eq $Checkpoint -or $null -eq $Checkpoint.Files) { return $false }
  if (-not $Checkpoint.Files.ContainsKey($FullName)) { return $false }

  $e = $Checkpoint.Files[$FullName]
  if ($null -eq $e) { return $false }

  return ([int64]$e.LastWriteUtcTicks -eq [int64]$Sig.LastWriteUtcTicks -and [int64]$e.Length -eq [int64]$Sig.Length)
}

function Checkpoint-MarkProcessed {
  param(
    $Checkpoint,
    [string]$FullName,
    $Sig
  )

  $Checkpoint.Files[$FullName] = [pscustomobject]@{
    LastWriteUtcTicks = [int64]$Sig.LastWriteUtcTicks
    Length            = [int64]$Sig.Length
    ProcessedUtc      = (Get-Date).ToUniversalTime().ToString("s")
  }
}

# -------------------- Cursor checkpoint --------------------
function Load-CheckpointCursor {
  param([string]$Path)

  if (-not (Test-Path $Path)) {
    return [pscustomobject]@{
      Version = "V8.1.2"
      UpdatedUtc = (Get-Date).ToUniversalTime().ToString("s")
      LastWriteUtcTicks = 0
      LastWriteUtc      = ""
    }
  }

  try {
    $raw = Get-Content -Raw -Path $Path
    if ([string]::IsNullOrWhiteSpace($raw)) { throw "Empty cursor checkpoint" }

    $obj = $raw | ConvertFrom-Json -Depth 20
    if ($null -eq $obj.LastWriteUtcTicks) { $obj | Add-Member -NotePropertyName LastWriteUtcTicks -NotePropertyValue 0 -Force }
    if ($null -eq $obj.LastWriteUtc)      { $obj | Add-Member -NotePropertyName LastWriteUtc      -NotePropertyValue "" -Force }
    return $obj
  } catch {
    Write-Warning "Checkpoint cursor load failed ($Path): $($_.Exception.Message). Starting at 0."
    return [pscustomobject]@{
      Version = "V8.1.2"
      UpdatedUtc = (Get-Date).ToUniversalTime().ToString("s")
      LastWriteUtcTicks = 0
      LastWriteUtc      = ""
    }
  }
}

function Save-CheckpointCursor {
  param($Cursor, [string]$Path)

  $payload = [pscustomobject]@{
    Version = "V8.1.2"
    UpdatedUtc = (Get-Date).ToUniversalTime().ToString("s")
    LastWriteUtcTicks = [int64]$Cursor.LastWriteUtcTicks
    LastWriteUtc      = [string]$Cursor.LastWriteUtc
  }

  $json = $payload | ConvertTo-Json -Depth 20
  Set-Content -Path $Path -Value $json -Encoding UTF8
}

# -------------------- Payload formatting helpers --------------------
function To-CentsInt {
  param($x)
  try {
    if ($null -eq $x) { return 0 }
    $d = [double]$x
    if ($d -le 0) { return 0 }
    return [int][math]::Round($d * 100.0, 0)
  } catch {
    return 0
  }
}

function To-BpInt {
  param($x)
  try {
    if ($null -eq $x) { return 0 }
    $d = [double]$x
    return [int][math]::Round($d * 10000.0, 0)
  } catch {
    return 0
  }
}

function Normalize-PayloadDateKey {
  param([string]$s)
  if ([string]::IsNullOrWhiteSpace($s)) { return $null }
  $t = $s.Trim()
  if ($t -match '^\d{8}$') {
    return ([datetime]::ParseExact($t, "yyyyMMdd", $null)).ToString("yyyy-MM-dd")
  }
  if ($t -match '^\d{4}-\d{2}-\d{2}$') { return $t }
  return $null
}

# -------------------- Main validation --------------------
if (-not (Test-Path $RootPath)) { throw "RootPath not found: $RootPath" }
if ($LookbackDays -lt 1) { throw "LookbackDays must be >= 1" }
if ($TopN -lt 1) { throw "TopN must be >= 1" }
if ($BandPct -le 0 -or $BandPct -ge 0.50) { throw "BandPct must be > 0 and < 0.50 (example: 0.05)" }
if ($SnapshotIntervalMinutes -lt 1) { throw "SnapshotIntervalMinutes must be >= 1" }
if ($MaxPayloadChars -lt 1000) { throw "MaxPayloadChars must be >= 1000" }
if ($MaxSegments -lt 1) { throw "MaxSegments must be >= 1" }
if ($StrikeMatchTolerance -lt 0) { throw "StrikeMatchTolerance must be >= 0" }

if ([string]::IsNullOrWhiteSpace($HistoricalPath)) {
  $HistoricalPath = Join-Path $RootPath "_Historical"
}

# Interpret effective timestamp choice (enum takes precedence)
$useWriteTime = $true
if ($ObservedTimestampSource -eq "FileName") { $useWriteTime = $false }
elseif ($ObservedTimestampSource -eq "WriteTimeUtc") { $useWriteTime = $true }
else { $useWriteTime = $true }

if ($PSBoundParameters.ContainsKey('UseFileWriteTimeAsObserved')) {
  $useWriteTime = [bool]$UseFileWriteTimeAsObserved
}

$cachePath = Join-Path $OutDir "_ZeroDTE_Strategy_Cache_V8_1_2.json"
$cache = Load-Cache -Path $cachePath

$checkpoint = Load-Checkpoint -Path $CheckpointPath
$cursor = Load-CheckpointCursor -Path $CheckpointCursorPath

[int64]$cursorTicks = 0
if ($UseCheckpointCursor -and -not $ForceReprocess) {
  try { $cursorTicks = [int64]$cursor.LastWriteUtcTicks } catch { $cursorTicks = 0 }
}

$tsSrcText = if ($useWriteTime) { "WriteTimeUtc (file write)" } else { "FileName (embedded)" }

if ($VerboseOutput) {
  Write-Host "============================================================="
  Write-Host "ZeroDTE Strategy -> Payload (V8.1.2)"
  Write-Host "Previous:                 V8.1.1"
  Write-Host "Symbol:                   $Symbol"
  Write-Host "RootPath:                 $RootPath"
  Write-Host "OutDir:                   $OutDir"
  Write-Host "PayloadOutDir:            $PayloadOutDir"
  Write-Host "IncludeHistorical:        $([bool]$IncludeHistorical)"
  Write-Host "HistoricalPath:           $HistoricalPath"
  Write-Host "HourlySnapshots (analysis): $([bool]$HourlySnapshots)"
  Write-Host "SnapshotIntervalMinutes:  $SnapshotIntervalMinutes"
  Write-Host "LookbackDays:             $LookbackDays"
  Write-Host "EmitPerDayPayloads:       $([bool]$EmitPerDayPayloads)"
  Write-Host "EmitHourlyPayloads:       $([bool]$EmitHourlyPayloads)"
  Write-Host "EmitDailyPayloads:        $([bool]$EmitDailyPayloads)"
  Write-Host "EmitPayloadKinds:         $EmitPayloadKinds"
  Write-Host "OnlyPayloadDate:          $OnlyPayloadDate"
  Write-Host "IncludeWeeklyExpInName:   $([bool]$IncludeWeeklyExpirationInFileName)"
  Write-Host "BandPct:                  $BandPct"
  Write-Host "StrikeMatchTolerance:     $StrikeMatchTolerance"
  Write-Host "CachePath:                $cachePath"
  Write-Host "ForceReprocess:           $([bool]$ForceReprocess)"
  Write-Host "Observed TS Src (eff):    $tsSrcText"
  Write-Host "CheckpointPath:           $CheckpointPath"
  Write-Host "UseCheckpointFastMode:    $([bool]$UseCheckpointFastMode)"
  Write-Host "UseCheckpointCursor:      $([bool]$UseCheckpointCursor)"
  Write-Host "CheckpointCursorPath:     $CheckpointCursorPath"
  Write-Host "Cursor LastWriteUtcTicks: $cursorTicks"
  Write-Host "MaxPayloadChars:          $MaxPayloadChars"
  Write-Host "MaxSegments:              $MaxSegments"
  Write-Host "============================================================="
}

# Collect files from RootPath (+ optional Historical)
$scanPaths = New-Object System.Collections.Generic.List[string]
$scanPaths.Add($RootPath) | Out-Null
if ($IncludeHistorical -and (Test-Path $HistoricalPath)) {
  $scanPaths.Add($HistoricalPath) | Out-Null
}

$itemsList = New-Object System.Collections.Generic.List[object]
[int64]$maxSeenWriteTicks = $cursorTicks

$minKeepDate = (Get-Date).Date.AddDays(-1 * ([math]::Max(1, ($LookbackDays + 2))))

foreach ($p in $scanPaths) {
  if ($VerboseOutput) { Write-Host "Scanning: $p" }

  foreach ($f in (Get-ChildItem -Path $p -File -Filter "*.json" -ErrorAction SilentlyContinue)) {
    $lwTicks = [int64]$f.LastWriteTimeUtc.Ticks
    if ($lwTicks -gt $maxSeenWriteTicks) { $maxSeenWriteTicks = $lwTicks }

    $meta0 = Parse-SnapshotFileName -FileName $f.Name
    if ($null -eq $meta0) { continue }

    $effObsDT =
      if ($useWriteTime) { [datetime]::SpecifyKind($f.LastWriteTimeUtc, [System.DateTimeKind]::Utc) }
      else { $meta0.ObservedDT }

    $effObsDate = $effObsDT.Date

    $skipByCursor = $false
    if ($UseCheckpointCursor -and -not $ForceReprocess -and $lwTicks -le $cursorTicks) {
      $skipByCursor = $true
    }

    if ($skipByCursor -and $effObsDate -lt $minKeepDate) {
      continue
    }

    $meta2 = [pscustomobject]@{
      Ticker             = $meta0.Ticker
      SpotInName         = $meta0.SpotInName
      Expiration         = $meta0.Expiration

      SourceObservedDT   = $meta0.ObservedDT
      SourceObservedDate = $meta0.ObservedDate

      EffectiveObservedDT   = $effObsDT
      EffectiveObservedDate = $effObsDate
    }

    $itemsList.Add([pscustomobject]@{ File = $f; Meta = $meta2 }) | Out-Null
  }
}

if ($itemsList.Count -eq 0) {
  $msg = "No matching snapshot files found under: $($scanPaths -join ', ')"
  Write-Host $msg
  return
}

$items = $itemsList | Sort-Object { $_.Meta.EffectiveObservedDT }

$seg0 = @{}
$segW = @{}
$segM = @{}

[int]$cacheHits = 0
[int]$cacheMiss = 0
[int]$cacheWrites = 0
[int]$filesConsidered = 0
[int]$filesUsed = 0
[int]$checkpointHits = 0
[int]$checkpointSkips = 0
[int]$checkpointMarks = 0

foreach ($it in $items) {
  $filesConsidered++
  $f    = $it.File
  $meta = $it.Meta

  $obsET = Convert-ToEastern $meta.EffectiveObservedDT
  $dateKey = $meta.EffectiveObservedDate.ToString("yyyy-MM-dd")

  $slotIndex = $null
  if ($HourlySnapshots) {
    $slotIndex = Get-RthSlotIndex -ObservedET $obsET -IntervalMinutes $SnapshotIntervalMinutes
    if ($null -eq $slotIndex) {
      continue
    }
  } else {
    $slotsToday = @(Get-RthSlotsForDateET -DateET $obsET.Date -IntervalMinutes $SnapshotIntervalMinutes)
    $rthStart = $slotsToday[0].StartET
    $rthEnd   = $slotsToday[$slotsToday.Count-1].EndET
    if ($obsET -lt $rthStart -or $obsET -ge $rthEnd) {
      continue
    }
  }

  $segKey = if ($HourlySnapshots) { "$dateKey|$SnapshotIntervalMinutes|$slotIndex" } else { $dateKey }

  $weeklyExp  = Get-WeeklyExpiration  -Date $meta.EffectiveObservedDate
  $monthlyExp = Get-MonthlyExpiration -Date $meta.EffectiveObservedDate

  $is0 = ($meta.Expiration -eq $meta.EffectiveObservedDate)
  $isW = ($meta.Expiration -eq $weeklyExp)
  $isM = ($meta.Expiration -eq $monthlyExp)

  if (-not ($is0 -or $isW -or $isM)) {
    continue
  }

  $filesUsed++
  $sig = Get-FileSig -File $f
  $key = $f.FullName

  $cpHit = Checkpoint-Hit -Checkpoint $checkpoint -FullName $key -Sig $sig
  if ($cpHit) {
    $checkpointHits++
  }

  $lvlLite = $null

  if ($cpHit -and $UseCheckpointFastMode -and -not $ForceReprocess) {
    $checkpointSkips++
    $useCacheFast = Cache-Hit -Cache $cache -Key $key -Sig $sig -ForceReprocess:$false
    if ($useCacheFast) {
      $cacheHits++
      $e = $cache.Entries[$key]
      $lvlLite    = Ensure-LevelProps $e.Level
      if (-not $cpHit) {
        Checkpoint-MarkProcessed -Checkpoint $checkpoint -FullName $key -Sig $sig
        $checkpointMarks++
      }
    } else {
      $cpHit = $false
    }
  }

  if ($null -eq $lvlLite) {
    $useCache = Cache-Hit -Cache $cache -Key $key -Sig $sig -ForceReprocess:$ForceReprocess
    if ($useCache) {
      $cacheHits++
      $e = $cache.Entries[$key]
      $lvlLite    = Ensure-LevelProps $e.Level

      if (-not $cpHit) {
        Checkpoint-MarkProcessed -Checkpoint $checkpoint -FullName $key -Sig $sig
        $checkpointMarks++
      }
    } else {
      $cacheMiss++

      $lvl = Compute-LevelsFromFile -FullName $f.FullName -Meta $meta -ContractMultiplier $ContractMultiplier -BandPct $BandPct -ObservedET $obsET -StrikeTol $StrikeMatchTolerance

      if ($ShowKeyDebug) {
        $json = Get-Content -Raw -Path $f.FullName | ConvertFrom-Json
        $expected = "optionSymbol","strike","side","openInterest","volume","underlyingPrice","gamma","iv","mid","midPrice","mark","bid","ask"
        $present  = $json.PSObject.Properties.Name
        Write-Host ("[$($f.Name)] present keys: " + (($expected | ForEach-Object { "$_=" + ($present -contains $_) }) -join ", "))
      }

      $lvlLite = [pscustomobject]@{
        Spot     = $lvl.Spot
        CallWall = $lvl.CallWall
        PutWall  = $lvl.PutWall
        Magnet   = $lvl.Magnet
        Flip     = $lvl.Flip
        Pressure = $lvl.Pressure

        CallWallAbsGEX = [double]$lvl.CallWallAbsGEX
        PutWallAbsGEX  = [double]$lvl.PutWallAbsGEX
        MagnetAbsGEX   = [double]$lvl.MagnetAbsGEX
        VegaNet        = [double]$lvl.VegaNet
        VegaAbs        = [double]$lvl.VegaAbs

        CallWallMid    = $lvl.CallWallMid
        PutWallMid     = $lvl.PutWallMid
        MagnetMid      = $lvl.MagnetMid
        FlipMid        = $lvl.FlipMid

        AtmIVCall   = $lvl.AtmIVCall
        AtmIVPut    = $lvl.AtmIVPut
        AtmIVMid    = $lvl.AtmIVMid
        AtmIVStrike = $lvl.AtmIVStrike

        IVUpper = $lvl.IVUpper
        IVLower = $lvl.IVLower
        IVMove  = $lvl.IVMove
        IVTYears= $lvl.IVTYears
      }

      $tsSrcShort = if ($useWriteTime) { "WriteTimeUtc" } else { "FileName" }

      $cache.Entries[$key] = [pscustomobject]@{
        LastWriteUtcTicks = $sig.LastWriteUtcTicks
        Length            = $sig.Length
        Meta              = [pscustomobject]@{
          Ticker       = $meta.Ticker
          SpotInName   = $meta.SpotInName
          Expiration   = $meta.Expiration.ToString("yyyy-MM-dd")
          EffectiveObservedDT   = $meta.EffectiveObservedDT.ToString("yyyy-MM-dd HH:mm:ss")
          EffectiveObservedDate = $meta.EffectiveObservedDate.ToString("yyyy-MM-dd")
          ObservedTimestampSource = $tsSrcShort
        }
        Level             = $lvlLite
        UpdatedUtc        = (Get-Date).ToUniversalTime().ToString("s")
      }
      $cacheWrites++

      Checkpoint-MarkProcessed -Checkpoint $checkpoint -FullName $key -Sig $sig
      $checkpointMarks++
    }
  }

  $lvlLite = Ensure-LevelProps $lvlLite
  $tsSrcShort = if ($useWriteTime) { "WriteTimeUtc" } else { "FileName" }

  $candidateBase = [pscustomobject]@{
    Key        = $segKey
    Ticker     = $meta.Ticker
    Date       = $dateKey
    SlotIndex  = $slotIndex
    ObservedDT = $meta.EffectiveObservedDT
    ObservedET = $obsET
    ObservedTimestampSource = $tsSrcShort
    Expiration = $meta.Expiration

    Spot       = $lvlLite.Spot
    CallWall   = $lvlLite.CallWall
    PutWall    = $lvlLite.PutWall
    Magnet     = $lvlLite.Magnet
    Flip       = $lvlLite.Flip
    Pressure   = $lvlLite.Pressure

    CallWallAbsGEX = [double]$lvlLite.CallWallAbsGEX
    PutWallAbsGEX  = [double]$lvlLite.PutWallAbsGEX
    MagnetAbsGEX   = [double]$lvlLite.MagnetAbsGEX
    VegaNet        = [double]$lvlLite.VegaNet
    VegaAbs        = [double]$lvlLite.VegaAbs

    CallWallMid = $lvlLite.CallWallMid
    PutWallMid  = $lvlLite.PutWallMid
    MagnetMid   = $lvlLite.MagnetMid
    FlipMid     = $lvlLite.FlipMid

    CallWallScore = 0
    PutWallScore  = 0
    MagnetScore   = 0

    IVUpper     = $lvlLite.IVUpper
    IVLower     = $lvlLite.IVLower
    AtmIVCall   = $lvlLite.AtmIVCall
    AtmIVPut    = $lvlLite.AtmIVPut
    AtmIVMid    = $lvlLite.AtmIVMid
    AtmIVStrike = $lvlLite.AtmIVStrike

    SourceFile = $f.Name
  }

  if ($is0) {
    if (-not $seg0.ContainsKey($segKey) -or $candidateBase.ObservedDT -lt $seg0[$segKey].ObservedDT) {
      $seg0[$segKey] = $candidateBase
    }
  }
  if ($isW) {
    if (-not $segW.ContainsKey($segKey) -or $candidateBase.ObservedDT -lt $segW[$segKey].ObservedDT) {
      $segW[$segKey] = $candidateBase
    }
  }
  if ($isM) {
    if (-not $segM.ContainsKey($segKey) -or $candidateBase.ObservedDT -lt $segM[$segKey].ObservedDT) {
      $segM[$segKey] = $candidateBase
    }
  }
}

Save-Cache -Cache $cache -Path $cachePath
Save-Checkpoint -Checkpoint $checkpoint -Path $CheckpointPath

if ($UseCheckpointCursor -and -not $ForceReprocess) {
  if ($maxSeenWriteTicks -gt $cursorTicks) {
    $cursor.LastWriteUtcTicks = $maxSeenWriteTicks

    # FIX (V8.1.2): ticks are DateTime ticks, not FILETIME
    try {
      $dtUtc = [datetime]::new([int64]$maxSeenWriteTicks, [System.DateTimeKind]::Utc)
      $cursor.LastWriteUtc = $dtUtc.ToString("s")
    } catch {
      $cursor.LastWriteUtc = (Get-Date).ToUniversalTime().ToString("s")
    }

    Save-CheckpointCursor -Cursor $cursor -Path $CheckpointCursorPath
  }
}

# -------------------- Build per-day payloads --------------------
function Build-SegmentsForDateKind {
  param(
    [string]$DateKey,
    [ValidateSet("0DTE","WEEKLY")]
    [string]$Kind,
    [ValidateSet("H","D")]
    [string]$ModeChar,
    [int]$IntervalMin,
    $Seg0,
    $SegW,
    $SegM
  )

  $dt = [datetime]::ParseExact($DateKey, "yyyy-MM-dd", $null)
  $slots = @(Get-RthSlotsForDateET -DateET $dt -IntervalMinutes $IntervalMin)

  $segs = New-Object System.Collections.Generic.List[object]

  if ($ModeChar -eq "D") {
    $key = $DateKey
    $hourlyKey = "$DateKey|$IntervalMin|0"

    $primary =
      if ($Kind -eq "0DTE") {
        if ($Seg0.ContainsKey($hourlyKey)) { $Seg0[$hourlyKey] } elseif ($Seg0.ContainsKey($key)) { $Seg0[$key] } else { $null }
      } else {
        if ($SegW.ContainsKey($hourlyKey)) { $SegW[$hourlyKey] } elseif ($SegW.ContainsKey($key)) { $SegW[$key] } else { $null }
      }

    $ctxM =
      if ($SegM.ContainsKey($hourlyKey)) { $SegM[$hourlyKey] } elseif ($SegM.ContainsKey($key)) { $SegM[$key] } else { $null }

    if ($null -ne $primary) {
      $segs.Add([pscustomobject]@{
        Date      = $DateKey
        SlotIndex = 0
        StartET   = $slots[0].StartET
        EndET     = $slots[$slots.Count-1].EndET
        RecP      = $primary
        RecM      = $ctxM
      }) | Out-Null
    }
  } else {
    foreach ($s in $slots) {
      $k = "$DateKey|$IntervalMin|$($s.SlotIndex)"
      $primary =
        if ($Kind -eq "0DTE") { if ($Seg0.ContainsKey($k)) { $Seg0[$k] } else { $null } }
        else                  { if ($SegW.ContainsKey($k)) { $SegW[$k] } else { $null } }

      $ctxM = if ($SegM.ContainsKey($k)) { $SegM[$k] } else { $null }

      if ($null -ne $primary) {
        $segs.Add([pscustomobject]@{
          Date      = $DateKey
          SlotIndex = $s.SlotIndex
          StartET   = $s.StartET
          EndET     = $s.EndET
          RecP      = $primary
          RecM      = $ctxM
        }) | Out-Null
      }
    }
  }

  return $segs
}

function Apply-StrengthScores {
  param($Segments)

  $segmentsChrono = @($Segments | Sort-Object { $_.StartET })
  $prevCallAbs = 0.0
  $prevPutAbs  = 0.0
  $prevMagAbs  = 0.0

  foreach ($seg in $segmentsChrono) {
    $r = $seg.RecP
    if ($null -eq $r) { continue }

    $curCall = 0.0; $curPut = 0.0; $curMag = 0.0
    try { $curCall = [double]$r.CallWallAbsGEX } catch { $curCall = 0.0 }
    try { $curPut  = [double]$r.PutWallAbsGEX  } catch { $curPut  = 0.0 }
    try { $curMag  = [double]$r.MagnetAbsGEX   } catch { $curMag  = 0.0 }

    $r.CallWallScore = Compute-MagnetScore -CurAbsGex $curCall -PrevAbsGex $prevCallAbs
    $r.PutWallScore  = Compute-MagnetScore -CurAbsGex $curPut  -PrevAbsGex $prevPutAbs
    $r.MagnetScore   = Compute-MagnetScore -CurAbsGex $curMag  -PrevAbsGex $prevMagAbs

    if ($curCall -gt 0) { $prevCallAbs = $curCall }
    if ($curPut  -gt 0) { $prevPutAbs  = $curPut  }
    if ($curMag  -gt 0) { $prevMagAbs  = $curMag  }
  }

  return $segmentsChrono
}

function Build-PayloadString {
  param(
    [ValidateSet("H","D")]
    [string]$ModeChar,
    [int]$IntervalMin,
    $SegmentsChrono,
    [ValidateSet("0DTE","WEEKLY")]
    [string]$Kind
  )

  $segmentsChrono = @($SegmentsChrono)
  if ($segmentsChrono.Count -gt $MaxSegments) {
    $segmentsChrono = @($segmentsChrono | Select-Object -Last $MaxSegments)
  }

  $records = New-Object System.Collections.Generic.List[string]

  foreach ($seg in $segmentsChrono) {
    $rP = $seg.RecP
    if ($null -eq $rP) { continue }

    $ds = [datetime]::ParseExact($seg.Date, "yyyy-MM-dd", $null).ToString("yyyyMMdd")
    $slot = if ($ModeChar -eq "H") { [int]$seg.SlotIndex } else { 0 }

    $cwC = To-CentsInt $rP.CallWall
    $pwC = To-CentsInt $rP.PutWall
    $mgC = To-CentsInt $rP.Magnet
    $flC = To-CentsInt $rP.Flip
    $prBp = To-BpInt $rP.Pressure

    $cwS = [int]$rP.CallWallScore
    $pwS = [int]$rP.PutWallScore
    $mgS = [int]$rP.MagnetScore

    $wcwC = $cwC
    $wpwC = $pwC
    $wmgC = $mgC
    $wflC = $flC

    $mcwC = 0; $mpwC = 0; $mmgC = 0; $mflC = 0
    if ($null -ne $seg.RecM) {
      $mcwC = To-CentsInt $seg.RecM.CallWall
      $mpwC = To-CentsInt $seg.RecM.PutWall
      $mmgC = To-CentsInt $seg.RecM.Magnet
      $mflC = To-CentsInt $seg.RecM.Flip
    }

    $ivUC = To-CentsInt $rP.IVUpper
    $ivLC = To-CentsInt $rP.IVLower

    $ivCallBp = To-BpInt $rP.AtmIVCall
    $ivPutBp  = To-BpInt $rP.AtmIVPut

    $vNetK = To-KInt $rP.VegaNet
    $vAbsK = To-KInt $rP.VegaAbs

    $cwMidC = To-CentsInt $rP.CallWallMid
    $pwMidC = To-CentsInt $rP.PutWallMid
    $mgMidC = To-CentsInt $rP.MagnetMid
    $flMidC = To-CentsInt $rP.FlipMid

    $rec = "{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10},{11},{12},{13},{14},{15},{16},{17},{18},{19},{20},{21},{22},{23},{24},{25},{26},{27}" -f `
      $ds, $slot, $cwC, $pwC, $mgC, $flC, $prBp, `
      $cwS, $pwS, $mgS, `
      $wcwC, $wpwC, $wmgC, $wflC, `
      $mcwC, $mpwC, $mmgC, $mflC, `
      $ivUC, $ivLC, `
      $ivCallBp, $ivPutBp, `
      $vNetK, $vAbsK, `
      $cwMidC, $pwMidC, $mgMidC, $flMidC

    $records.Add($rec) | Out-Null
  }

  $header = "8.1.2~{0}~{1}~" -f $ModeChar, $IntervalMin
  $payload = $header + ($records -join "^")

  if ($payload.Length -gt $MaxPayloadChars) {
    $recsArr = @($records)
    while ($recsArr.Count -gt 1) {
      $recsArr = @($recsArr | Select-Object -Skip 1)
      $payloadTry = $header + ($recsArr -join "^")
      if ($payloadTry.Length -le $MaxPayloadChars) {
        $payload = $payloadTry
        break
      }
    }
  }

  return $payload
}

function Write-PerDayPayloads {
  param(
    [string]$DateKey,
    [ValidateSet("0DTE","WEEKLY")]
    [string]$Kind,
    [ValidateSet("H","D")]
    [string]$ModeChar
  )

  $dt = [datetime]::ParseExact($DateKey, "yyyy-MM-dd", $null)
  $dateTag = $dt.ToString("yyyyMMdd")

  $weeklyExp = Get-WeeklyExpiration -Date $dt
  $expTag = $weeklyExp.ToString("yyyyMMdd")

  $segs = Build-SegmentsForDateKind -DateKey $DateKey -Kind $Kind -ModeChar $ModeChar -IntervalMin $SnapshotIntervalMinutes -Seg0 $seg0 -SegW $segW -SegM $segM
  if ($null -eq $segs -or $segs.Count -eq 0) { return $null }

  $segsScored = Apply-StrengthScores -Segments $segs
  $payload = Build-PayloadString -ModeChar $ModeChar -IntervalMin $SnapshotIntervalMinutes -SegmentsChrono $segsScored -Kind $Kind

  $modeTag = if ($ModeChar -eq "H") { "H$SnapshotIntervalMinutes" } else { "D$SnapshotIntervalMinutes" }

  $fn =
    if ($Kind -eq "WEEKLY" -and $IncludeWeeklyExpirationInFileName) {
      ("Payload_V8_1_2_{0}_{1}_{2}_EXP{3}_{4}.txt" -f $Symbol, $Kind, $dateTag, $expTag, $modeTag)
    } else {
      ("Payload_V8_1_2_{0}_{1}_{2}_{3}.txt" -f $Symbol, $Kind, $dateTag, $modeTag)
    }

  $outPath = Join-Path $PayloadOutDir $fn
  Set-Content -Path $outPath -Value $payload -Encoding UTF8
  return [pscustomobject]@{ Date=$DateKey; Kind=$Kind; Mode=$ModeChar; Path=$outPath; Length=$payload.Length }
}

# Decide which dates to emit
$allDates0 = @($seg0.Values | Select-Object -ExpandProperty Date -Unique)
$allDatesW = @($segW.Values | Select-Object -ExpandProperty Date -Unique)
$allDatesAll = @($allDates0 + $allDatesW | Sort-Object -Unique)

if ($allDatesAll.Count -eq 0) {
  Write-Host ""
  Write-Host "No segments found for payload output. (V8.1.2)"
  return
}

$onlyKey = Normalize-PayloadDateKey -s $OnlyPayloadDate

$wantDates =
  if ($null -ne $onlyKey) {
    @($allDatesAll | Where-Object { $_ -eq $onlyKey })
  } else {
    @($allDatesAll | Sort-Object | Select-Object -Last $LookbackDays)
  }

if ($wantDates.Count -eq 0) {
  Write-Host ""
  Write-Host "No matching payload dates found to emit. (V8.1.2)"
  if ($null -ne $onlyKey) { Write-Host "OnlyPayloadDate requested: $onlyKey" }
  return
}

$emitKinds = @()
switch ($EmitPayloadKinds) {
  "0DTE"   { $emitKinds = @("0DTE") }
  "WEEKLY" { $emitKinds = @("WEEKLY") }
  default  { $emitKinds = @("0DTE","WEEKLY") }
}

$emitModes = @()
if ($EmitHourlyPayloads) { $emitModes += "H" }
if ($EmitDailyPayloads)  { $emitModes += "D" }

$written = New-Object System.Collections.Generic.List[object]

if ($EmitPerDayPayloads) {
  foreach ($d in $wantDates) {
    foreach ($k in $emitKinds) {
      foreach ($m in $emitModes) {
        $r = Write-PerDayPayloads -DateKey $d -Kind $k -ModeChar $m
        if ($null -ne $r) { $written.Add($r) | Out-Null }
      }
    }
  }
}

# Legacy combined payload copy
if ($CopyPayloadToClipboard -and $written.Count -gt 0) {
  $last = $written | Sort-Object Date, Kind, Mode | Select-Object -Last 1
  $txt = Get-Content -Raw -Path $last.Path
  try {
    Set-Clipboard -Value $txt
    if ($VerboseOutput) { Write-Host "Copied latest payload to clipboard: $($last.Path)" }
  } catch {
    Write-Warning "Failed to copy payload to clipboard: $($_.Exception.Message)"
  }

  try {
    Set-Content -Path $PayloadOutPath -Value $txt -Encoding UTF8
  } catch { }
}

Write-Host ""
Write-Host "Done. (V8.1.2)"
Write-Host ("OutDir:            {0}" -f $OutDir)
Write-Host ("PayloadOutDir:     {0}" -f $PayloadOutDir)
Write-Host ("Files considered:  {0} | used: {1} | cache hits: {2} | processed: {3} | cache writes: {4}" -f $filesConsidered, $filesUsed, $cacheHits, $cacheMiss, $cacheWrites)
Write-Host ("Checkpoint hits:   {0} | fast-skips: {1} | marks: {2}" -f $checkpointHits, $checkpointSkips, $checkpointMarks)
Write-Host ("Cache saved:       {0}" -f $cachePath)
Write-Host ("Checkpoint saved:  {0}" -f $CheckpointPath)
if ($UseCheckpointCursor) {
  Write-Host ("Cursor saved:      {0} | ticks={1}" -f $CheckpointCursorPath, ([int64]$cursor.LastWriteUtcTicks))
}
Write-Host ""
if ($written.Count -gt 0) {
  Write-Host "Payload files written:"
  foreach ($w in ($written | Sort-Object Date, Kind, Mode)) {
    Write-Host (" - {0} {1} {2} | len={3} | {4}" -f $w.Date, $w.Kind, $w.Mode, $w.Length, $w.Path)
  }
} else {
  Write-Host "No payload files were written (EmitPerDayPayloads is OFF or no segments matched)."
}
Write-Host ""
Write-Host "Paste any payload file contents into your Pine input field: Payload (V8.1.2)"
