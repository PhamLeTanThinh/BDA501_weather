#!/usr/bin/env bash
# Chạy job climatology bằng Hadoop Streaming trên cụm lab (HDFS + YARN, container bda501-client).
# Cần: cụm BDA501-Hadoop-Lab đang chạy; đã chạy `src.mapreduce.mr_io export`.
#   scripts/run_mapreduce.sh            → data/mr/output/part-*, reports/mapreduce/job.log
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
C=bda501-client
HD=/rain-forecast/mr
RUNAS="$(docker exec $C id -un)"
# docker cp ghi file với quyền root: dọn và trả quyền bằng root
MSYS_NO_PATHCONV=1 docker exec -u root $C bash -c "rm -rf /tmp/mrjob /tmp/mrout && mkdir -p /tmp/mrjob"
docker cp "$ROOT/data/mr/input" "$C:/tmp/mrjob/"
docker cp "$ROOT/src/mapreduce/clim_mapper.py" "$C:/tmp/mrjob/"
docker cp "$ROOT/src/mapreduce/clim_reducer.py" "$C:/tmp/mrjob/"
MSYS_NO_PATHCONV=1 docker exec -u root $C chown -R "$RUNAS" /tmp/mrjob
mkdir -p "$ROOT/reports/mapreduce"
MSYS_NO_PATHCONV=1 docker exec $C bash -c "
set -e
cd /tmp/mrjob
hdfs dfs -rm -r -f -skipTrash $HD >/dev/null
hdfs dfs -mkdir -p $HD/input
hdfs dfs -put -f input/part-*.csv $HD/input/
hdfs dfs -ls -h $HD/input
# reducer --final chỉ chạy ở reduce; combiner dùng cùng script ở chế độ cộng dồn
time hadoop jar /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.5.0.jar \
  -D mapreduce.job.name=rain_climatology_location_month \
  -D mapreduce.job.reduces=2 \
  -D mapreduce.input.fileinputformat.split.maxsize=4194304 \
  -files clim_mapper.py,clim_reducer.py \
  -mapper 'python3 clim_mapper.py' \
  -combiner 'python3 clim_reducer.py' \
  -reducer 'python3 clim_reducer.py --final' \
  -input $HD/input -output $HD/output
hdfs dfs -ls $HD/output
" 2>&1 | tee "$ROOT/reports/mapreduce/job.log"
rm -rf "$ROOT/data/mr/output"
MSYS_NO_PATHCONV=1 docker exec $C bash -c "hdfs dfs -get $HD/output /tmp/mrout"
docker cp "$C:/tmp/mrout" "$ROOT/data/mr/output"
echo "output → data/mr/output"
