mkdir -p data
mkdir -p storage
mkdir -p tracefile

mkdir -p res
NUM=10
for i in $(seq 0 $NUM)
    do
        mkdir -p res/$i
    done

cd small_tools/
python generate_file.py 1 100 M