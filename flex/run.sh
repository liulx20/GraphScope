export FLEX_DATA_DIR=/data/30_32p
nohup rt_server -g /root/llx/flex_ldbc_snb/configs/graph.yaml -d /data/30_32p -l /root/llx/flex_ldbc_snb/configs/bulk_load.yaml -s 48 &

