"""Mapper/reducer chạy như Hadoop Streaming (map → sort → reduce); combiner không làm đổi kết quả."""
import random
import subprocess
import sys
from pathlib import Path

MR = Path(__file__).resolve().parents[1] / "src" / "mapreduce"


def run(script, lines, *args):
    p = subprocess.run([sys.executable, str(MR / script), *args], input="\n".join(lines) + "\n",
                       capture_output=True, text=True, check=True)
    return [l for l in p.stdout.splitlines() if l]


def pipeline(rows, combine_chunks=None):
    mapped = run("clim_mapper.py", rows)
    if combine_chunks:  # mỗi mapper tự combine phần của mình, như Hadoop
        size = len(mapped) // combine_chunks + 1
        mapped = [l for i in range(0, len(mapped), size)
                  for l in run("clim_reducer.py", sorted(mapped[i:i + size]))]
    return sorted(run("clim_reducer.py", sorted(mapped), "--final"))


def test_known_aggregate():
    rows = ["A,9,1,10.0", "A,9,0,2.0", "A,9,0,", "A,10,0,0.5", "B,9,1,30.5", "location_id,month,label,precipitation"]
    assert pipeline(rows) == ["A|09\t2,1,0.500000,6.000000,10.0",
                              "A|10\t1,0,0.000000,0.500000,0.5",
                              "B|09\t1,1,1.000000,30.500000,30.5"]


def test_combiner_does_not_change_result():
    rng = random.Random(7)
    rows = [f"{rng.choice('ABC')},{rng.randint(1, 12)},{int(rng.random() < 0.05)},{rng.random() * 20:.1f}"
            for _ in range(3000)]
    assert pipeline(rows) == pipeline(rows, combine_chunks=5)
