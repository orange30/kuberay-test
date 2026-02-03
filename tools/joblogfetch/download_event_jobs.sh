#!/bin/bash
# 从 COS 下载 event_JOBS.log 并解析内容

set -e

echo "=== 下载并查看 event_JOBS.log ==="
echo ""

# COS 配置
CLUSTER="rayjob-job-ve4ouffo-bbwrr_mce-proj-bk0lwhh1"
SESSION="session_2026-01-31_03-41-12_526238_1"
NODE="9da758bbb610dd76c280cc24981575544356164c321bec114ab6c6db"

# 构建路径
REMOTE_PATH="mce-proj-bk0lwhh1/${CLUSTER}/${SESSION}/logs/${NODE}/events/event_JOBS.log"
LOCAL_PATH="/tmp/event_JOBS.log"

echo "1. 下载 event_JOBS.log..."
echo "   远程路径: ${REMOTE_PATH}"
echo "   本地路径: ${LOCAL_PATH}"
echo ""

# 检查环境变量
if [ -z "$COS_BUCKET_URL" ]; then
    echo "❌ 错误: 未设置 COS_BUCKET_URL 环境变量"
    exit 1
fi

# 使用 coscli 下载（如果有的话）
if command -v coscli &> /dev/null; then
    coscli cp "cos://${REMOTE_PATH}" "$LOCAL_PATH"
else
    echo "   ⚠️  未找到 coscli，尝试使用 Go SDK..."
    
    # 使用 Go 程序下载
    cat > /tmp/download_event_jobs.go << 'EOF'
package main

import (
    "context"
    "fmt"
    "io"
    "net/http"
    "net/url"
    "os"
    "time"

    cos "github.com/tencentyun/cos-go-sdk-v5"
)

func main() {
    bucketURL := os.Getenv("COS_BUCKET_URL")
    secretID := os.Getenv("COS_SECRET_ID")
    secretKey := os.Getenv("COS_SECRET_KEY")
    remotePath := os.Args[1]
    localPath := os.Args[2]

    u, _ := url.Parse(bucketURL)
    b := &cos.BaseURL{BucketURL: u}
    client := cos.NewClient(b, &http.Client{
        Timeout: 60 * time.Second,
        Transport: &cos.AuthorizationTransport{
            SecretID:  secretID,
            SecretKey: secretKey,
        },
    })

    resp, err := client.Object.Get(context.Background(), remotePath, nil)
    if err != nil {
        fmt.Fprintf(os.Stderr, "下载失败: %v\n", err)
        os.Exit(1)
    }
    defer resp.Body.Close()

    f, err := os.Create(localPath)
    if err != nil {
        fmt.Fprintf(os.Stderr, "创建本地文件失败: %v\n", err)
        os.Exit(1)
    }
    defer f.Close()

    _, err = io.Copy(f, resp.Body)
    if err != nil {
        fmt.Fprintf(os.Stderr, "写入文件失败: %v\n", err)
        os.Exit(1)
    }

    fmt.Println("✅ 下载成功")
}
EOF

    go run /tmp/download_event_jobs.go "$REMOTE_PATH" "$LOCAL_PATH"
    rm /tmp/download_event_jobs.go
fi

echo ""
echo "2. 文件信息:"
ls -lh "$LOCAL_PATH"
echo ""

echo "3. 文件内容（前 20 行）:"
head -20 "$LOCAL_PATH"
echo ""

echo "4. 统计信息:"
echo "   总行数: $(wc -l < "$LOCAL_PATH")"
echo "   文件大小: $(du -h "$LOCAL_PATH" | awk '{print $1}')"
echo ""

echo "5. 解析 Job 信息:"
if command -v jq &> /dev/null; then
    echo "   Job 数量: $(grep -c '{"event_type"' "$LOCAL_PATH" || echo 0)"
    echo ""
    echo "   Job 列表:"
    cat "$LOCAL_PATH" | jq -r '. | "\(.job_id) - \(.status) - \(.submission_id)"' 2>/dev/null || echo "   解析失败"
else
    echo "   ⚠️  未安装 jq，无法解析 JSON"
    echo "   原始内容:"
    cat "$LOCAL_PATH"
fi

echo ""
echo "=== 完成 ==="
echo ""
echo "提示: 完整内容保存在 $LOCAL_PATH"
