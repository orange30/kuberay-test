#!/bin/bash
# 最简单的本地启动命令示例

# === 方式 1: 纯命令行（最直接） ===
S3DISABLE_SSL=true \
AWS_S3ID=minioadmin \
AWS_S3SECRET=minioadmin \
S3_BUCKET=ray-historyserver \
S3_ENDPOINT=192.168.129.137:30900 \
S3_REGION=test \
S3FORCE_PATH_STYLE=true \
./output/bin/historyserver \
  --runtime-class-name=s3 \
  --ray-root-dir=log \
  --dashboard-dir=./dashboard

# === 方式 2: 使用 export（推荐调试） ===
# export S3DISABLE_SSL=true
# export AWS_S3ID=minioadmin
# export AWS_S3SECRET=minioadmin
# export S3_BUCKET=ray-historyserver
# export S3_ENDPOINT=192.168.129.137:30900
# export S3_REGION=test
# export S3FORCE_PATH_STYLE=true
#
# ./output/bin/historyserver \
#   --runtime-class-name=s3 \
#   --ray-root-dir=log \
#   --dashboard-dir=./dashboard

# === 方式 3: 使用配置文件 ===
# export AWS_S3ID=minioadmin
# export AWS_S3SECRET=minioadmin
#
# ./output/bin/historyserver \
#   --runtime-class-name=s3 \
#   --ray-root-dir=log \
#   --dashboard-dir=./dashboard \
#   --runtime-class-config-path=./local-config.json
