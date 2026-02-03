#!/bin/bash
# 快速检查 COS 存储结构

set -e

if [ -z "$COS_BUCKET_URL" ] || [ -z "$COS_SECRET_ID" ] || [ -z "$COS_SECRET_KEY" ]; then
    echo "错误: 请先设置 COS 环境变量"
    echo "export COS_BUCKET_URL='...'"
    echo "export COS_SECRET_ID='...'"
    echo "export COS_SECRET_KEY='...'"
    exit 1
fi

CLUSTER_KEY="rayjob-job-ve4ouffo-bbwrr_mce-proj-bk0lwhh1"
SESSION="session_2026-01-31_03-41-12_526238_1"
ROOT_DIR="mce-proj-bk0lwhh1"

echo "=== 检查 COS 存储结构 ==="
echo ""
echo "检查集群: $CLUSTER_KEY"
echo "检查会话: $SESSION"
echo ""

cd /Users/zhikuodu/work/work_ray/kuberay-test/tools/cos_inspect

echo "1. 检查 metadir 结构..."
go run . --ray-root-dir "$ROOT_DIR" --cluster "$CLUSTER_KEY" --max-print 10

echo ""
echo "2. 检查完整数据结构（包括 job_events）..."
go run . --ray-root-dir "$ROOT_DIR" --cluster "$CLUSTER_KEY" --session "$SESSION" --max-print 20

echo ""
echo "=== 检查完成 ==="
