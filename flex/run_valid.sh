export FLEX_DATA_DIR=/data/sf10_validation/sf10_validation
nohup rt_server -g /root/llx/flex_ldbc_snb/configs/graph.yaml -d /data/sf10_validation/sf10_validation -l /root/llx/flex_ldbc_snb/configs/bulk_load.yaml -s 48 &

