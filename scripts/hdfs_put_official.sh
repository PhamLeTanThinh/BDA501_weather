#!/usr/bin/env bash
# Đưa raw CDS (zip NetCDF) và bronze (Parquet) của bản chính thức lên HDFS của cụm lab.
# File được truyền thẳng qua stdin của `hdfs dfs -put -` nên không cần chép tạm vào container.
# File đã có trên HDFS với cùng kích thước thì bỏ qua (chạy lại để tiếp tục).
#   scripts/hdfs_put_official.sh            → reports/official/hdfs_ls.txt
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DATA="${BDA_DATA_HOST:-D:/BDA501_data}"
C=bda501-client
HD=/rain-forecast

hdfs() { MSYS_NO_PATHCONV=1 docker exec -i "$C" hdfs "$@"; }

# kích thước các file đã có trên HDFS: "đường dẫn kích thước"
existing=$(MSYS_NO_PATHCONV=1 docker exec "$C" bash -c "hdfs dfs -ls -R $HD 2>/dev/null | awk '\$1 !~ /^d/ {print \$8, \$5}'" || true)

put_tree() {  # thư mục local, thư mục HDFS, mẫu file
    local src=$1 dst=$2 pattern=$3 n=0 skipped=0
    # đọc danh sách file từ fd 3: `docker exec -i` bên trong vòng lặp sẽ nuốt stdin
    while IFS= read -r -u 3 f; do
        rel=${f#"$src"/}
        target="$dst/$rel"
        size=$(stat -c %s "$f")
        if grep -qxF "$target $size" <<<"$existing"; then skipped=$((skipped + 1)); continue; fi
        hdfs dfs -mkdir -p "$(dirname "$target")" < /dev/null
        hdfs dfs -put -f - "$target" < "$f"
        n=$((n + 1))
    done 3< <(find "$src" -path "$src/year=*" -type f -name "$pattern" | sort)  # chỉ year=*, bỏ _7vars_old
    echo "$dst: $n file mới, $skipped file đã có"
}

put_tree "$DATA/raw/era5_cds" "$HD/raw/era5_cds" "era5_vn_*.zip"
put_tree "$DATA/lake/bronze/era5_cds_hourly" "$HD/bronze/era5_cds_hourly" "*.parquet"
mkdir -p "$ROOT/reports/official"
MSYS_NO_PATHCONV=1 docker exec "$C" bash -c "
hdfs dfs -du -s -h $HD/raw/era5_cds $HD/bronze/era5_cds_hourly
echo
hdfs dfs -count -h $HD/raw/era5_cds $HD/bronze/era5_cds_hourly
echo
hdfs dfs -ls -R $HD/raw/era5_cds | head -20
" | tee "$ROOT/reports/official/hdfs_ls.txt"
