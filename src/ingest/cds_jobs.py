"""Tách CDS thành hai bước: gửi request (chỉ chờ successful) và tải file về sau.

Bước tải qua mạng chậm có thể mất cả giờ cho một file; nếu gộp chung thì CDS đứng chờ. Ở đây:

  submit    gửi từng khối tuần tự, chờ CDS báo successful, ghi request_id vào manifest rồi gửi khối kế tiếp
            ngay (CDS chỉ cho mỗi tài khoản một request trong hàng đợi). Không tải gì.
  download  đọc manifest, tải các khối successful chưa có trên đĩa (ghi .part, đúng kích thước mới đổi tên).

Manifest là CSV chỉ ghi thêm; dòng cuối của mỗi file là trạng thái hiện tại. Chạy lại submit sẽ chờ tiếp
request còn dở trong manifest thay vì gửi trùng. Kết quả nằm trong cache của CDS có thời hạn; khối nào
tải không được nữa bị đánh dấu expired và lần submit sau sẽ gửi lại.

    python -m src.ingest.cds_jobs submit --years 2015 2020 --months-per-request 2 --manifest jobs_main.csv
    python -m src.ingest.cds_jobs download --manifest jobs_main.csv
"""
import argparse
import csv
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from cdsapi.api import get_url_key_verify
from ecmwf.datastores import Client

from src.ingest.download_era5_cds import AREA_VN, DATASET, chunks, months_on_disk, request

FIELDS = ["logged_utc", "file", "year", "months", "request_id", "status", "bytes", "note"]
PENDING = {"accepted", "running"}
POLL_SECONDS = 60


def client():
    url, key, verify = get_url_key_verify(None, None, None)  # đọc CDSAPI_RC hoặc ~/.cdsapirc
    return Client(url=url, key=key, verify=bool(verify))


def load_manifest(path):
    """file → dòng cuối cùng của file đó."""
    if not path.exists():
        return {}
    with open(path, newline="", encoding="utf-8") as f:
        return {r["file"]: r for r in csv.DictReader(f)}


def log_row(path, **row):
    new = not path.exists()
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, FIELDS)
        if new:
            w.writeheader()
        w.writerow({"logged_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"), **row})
    print(f"{row['file']}: {row['status']} {row.get('note', '')}", flush=True)


def file_name(year, months, day=None):
    if day:
        return f"era5_vn_{year}-{months[0]:02d}-{day}.zip"
    return f"era5_vn_{year}-{months[0]:02d}" + (f"_{months[-1]:02d}" if len(months) > 1 else "") + ".zip"


def months_of(name, year):
    m = re.fullmatch(rf"era5_vn_{year}-(\d\d)(?:_(\d\d))?\.zip", name)
    return set(range(int(m.group(1)), int(m.group(2) or m.group(1)) + 1)) if m else set()


def wait(c, manifest, row):
    """Chờ một request tới trạng thái cuối, ghi vào manifest, trả về trạng thái."""
    t0 = time.time()
    while True:
        try:
            remote = c.get_remote(row["request_id"])
            status = remote.status
        except Exception as e:  # noqa: BLE001 — lỗi mạng tạm thời: chờ rồi hỏi lại
            print(f"{row['file']}: lỗi khi hỏi trạng thái ({str(e)[:120]}), thử lại", flush=True)
            time.sleep(POLL_SECONDS)
            continue
        if status not in PENDING:
            break
        time.sleep(POLL_SECONDS)
    size, note = "", f"{time.time() - t0:.0f} s"
    if status == "successful":
        size = remote.get_results().content_length
    else:
        note += " " + str(remote.json.get("metadata", {}).get("results", ""))[:250]
    log_row(manifest, file=row["file"], year=row["year"], months=row["months"],
            request_id=row["request_id"], status=status, bytes=size, note=note)
    return status, note


def submit_one(c, manifest, out, year, months, day=None):
    name = file_name(year, months, day)
    req = request(year, AREA_VN, months, day)
    remote = c.submit(DATASET, req)  # lỗi ở đây (licence, quota…) làm dừng cả script: tránh gửi sai lặp lại
    row = dict(file=name, year=year, months=f"{months[0]:02d}-{months[-1]:02d}", request_id=remote.request_id)
    log_row(manifest, **row, status="accepted", bytes="", note="")
    return wait(c, manifest, row)


def cmd_submit(args):
    out, manifest = Path(args.out), Path(args.out) / args.manifest
    c = client()
    # request còn dở từ lần chạy trước (hoặc được ghi tay): chờ xong rồi mới gửi tiếp
    for row in load_manifest(manifest).values():
        if row["status"] in PENDING:
            print(f"{row['file']}: đang chờ request cũ {row['request_id']}", flush=True)
            wait(c, manifest, row)

    def done(name, year):
        r = load_manifest(manifest).get(name)
        return (out / f"year={year}" / name).exists() or (r is not None and r["status"] == "successful")

    if not args.no_lead_day:
        lead = args.years[0] - 1
        if not done(file_name(lead, [12], "31"), lead):
            submit_one(c, manifest, out, lead, [12], "31")
    for year in range(args.years[0], args.years[1] + 1):
        have = months_on_disk(out / f"year={year}", year)
        for name, r in load_manifest(manifest).items():
            if r["status"] == "successful":
                have |= months_of(name, year)
        # tháng mà tài khoản khác đã gửi (đang chờ hoặc xong) thì bỏ qua, tránh tải trùng giữa các tài khoản
        for other in out.glob("jobs_*.csv"):
            if other != manifest:
                for name, r in load_manifest(other).items():
                    if r["status"] in PENDING | {"successful"}:
                        have |= months_of(name, year)
        missing = [m for m in range(args.months[0], args.months[1] + 1) if m not in have]
        for block in chunks(missing, args.months_per_request):
            status, note = submit_one(c, manifest, out, year, block)
            if status != "successful" and "too large" in note and len(block) > 1:
                for m in block:
                    submit_one(c, manifest, out, year, [m])
    print("Đã gửi xong mọi khối; chạy `download` để tải.", flush=True)


def cmd_download(args):
    out, manifest = Path(args.out), Path(args.out) / args.manifest
    c = client()
    for name, row in load_manifest(manifest).items():
        dest = out / f"year={row['year']}" / name
        if row["status"] != "successful" or dest.exists():
            continue
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix(".part")
        t0 = time.time()
        try:
            results = c.get_results(row["request_id"])
        except Exception as e:  # noqa: BLE001 — kết quả đã bị xóa khỏi cache: để submit gửi lại
            log_row(manifest, **{k: row[k] for k in ("file", "year", "months", "request_id")},
                    status="expired", bytes="", note=str(e)[:250])
            continue
        expected = results.content_length
        tmp.unlink(missing_ok=True)
        results.download(str(tmp))
        size = tmp.stat().st_size
        if size != expected:
            print(f"{name}: tải thiếu ({size}/{expected} byte), giữ .part, chạy lại để thử lại", flush=True)
            continue
        tmp.replace(dest)
        print(f"{name}: ok, {size / 1e6:.1f} MB, {time.time() - t0:.0f} s", flush=True)
    print("Xong vòng tải.", flush=True)


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("submit", "download"):
        p = sub.add_parser(name)
        p.add_argument("--out", default="D:/BDA501_data/raw/era5_cds")
        p.add_argument("--manifest", required=True, help="tên file CSV trong <out>, mỗi tài khoản một file")
    s = sub.choices["submit"]
    s.add_argument("--years", nargs=2, type=int, default=[2015, 2025])
    s.add_argument("--months", nargs=2, type=int, default=[1, 12],
                   help="chỉ gửi các tháng trong khoảng này của mỗi năm (để chia một năm cho nhiều tài khoản)")
    s.add_argument("--months-per-request", type=int, default=2)
    s.add_argument("--no-lead-day", action="store_true")
    args = ap.parse_args()
    {"submit": cmd_submit, "download": cmd_download}[args.cmd](args)


if __name__ == "__main__":
    main()
