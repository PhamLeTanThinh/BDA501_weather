# Bật lại 6 job gửi request CDS sau khi máy khởi động lại. Job nào đang chạy thì bỏ qua.
# Tài khoản: mặc định = Thịnh, luan = Luân, team3 = Việt, team4 = Long, team5 = Bách, tan = Tân.
# Mỗi job chờ tiếp request cũ trong manifest trước khi gửi khối mới, nên không gửi trùng.
# Mỗi job chạy trong một cửa sổ cmd ẩn riêng: đóng cửa sổ start_jobs.bat không làm dừng job.
$repo = Split-Path -Parent $PSScriptRoot
$out = 'D:\BDA501_data\raw\era5_cds'
$py = "$repo\.venv\Scripts\python.exe"
$submit = "`"$py`" -m src.ingest.cds_jobs submit --months-per-request 2"
$running = (Get-CimInstance Win32_Process -Filter "Name='python.exe'").CommandLine -join "`n"

function Start-Job4($manifest, $log, $rc, $cmds) {
    if ($running -match [regex]::Escape($manifest)) { Write-Host "${manifest}: dang chay, bo qua"; return }
    $env = if ($rc) { "set CDSAPI_RC=$rc&& " } else { "" }
    # mỗi lệnh ghi thêm vào log; && để lượt sau chỉ chạy khi lượt trước xong không lỗi
    $chain = ($cmds | ForEach-Object { "$_ >> `"$out\$log`" 2>&1" }) -join ' && '
    # bọc cả chuỗi trong một cặp ngoặc kép: cmd /c bỏ cặp ngoặc ngoài cùng, giữ nguyên ngoặc của đường dẫn
    Start-Process cmd.exe -WindowStyle Hidden -WorkingDirectory $repo -ArgumentList "/c `"$env$chain`""
    Write-Host "${manifest}: da bat"
}

Start-Job4 jobs_main.csv submit_main.out '' @("$submit --years 2025 2025 --months 9 10 --no-lead-day --manifest jobs_main.csv")
Start-Job4 jobs_luan.csv submit_luan.out 'C:\Users\ADMIN\.cdsapirc_luan' @("$submit --years 2016 2016 --months 11 12 --no-lead-day --manifest jobs_luan.csv")
Start-Job4 jobs_team3.csv submit_team3.out 'C:\Users\ADMIN\.cdsapirc_team3' @("$submit --years 2025 2025 --months 11 12 --no-lead-day --manifest jobs_team3.csv")
Start-Job4 jobs_team4.csv submit_team4.out 'C:\Users\ADMIN\.cdsapirc_team4' @("$submit --years 2025 2025 --months 7 8 --no-lead-day --manifest jobs_team4.csv")
Start-Job4 jobs_team5.csv submit_team5.out 'C:\Users\ADMIN\.cdsapirc_team5' @("$submit --years 2020 2020 --months 9 10 --no-lead-day --manifest jobs_team5.csv")
Start-Job4 jobs_tan.csv submit_tan.out 'C:\Users\ADMIN\.cdsapirc_tan' @(
    "$submit --years 2023 2024 --no-lead-day --manifest jobs_tan.csv",
    "$submit --years 2020 2020 --months 5 8 --no-lead-day --manifest jobs_tan.csv")
