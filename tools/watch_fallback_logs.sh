#!/bin/bash
# 实时监控 FallbackToWorkerLogs 相关日志

echo "=========================================="
echo "Watching FallbackToWorkerLogs Logs"
echo "=========================================="
echo ""
echo "📝 Monitoring keywords:"
echo "   - FallbackToWorkerLogs"
echo "   - worker-*.out"
echo "   - reconstructed"
echo "   - tasks/actors count"
echo ""
echo "=========================================="
echo ""

# 如果有保存的日志文件，从文件读取；否则从 stdout
if [ -f /tmp/historyserver.log ]; then
    echo "📂 Reading from /tmp/historyserver.log"
    echo ""
    tail -f /tmp/historyserver.log | grep --line-buffered -E "FallbackToWorkerLogs|worker-.*\.out|reconstructed|tasks=|actors=|📁|📊|🔍|📄|✅|❌|⚠️|🎉"
else
    echo "⚠️  Log file not found: /tmp/historyserver.log"
    echo ""
    echo "Please run historyserver with output redirect:"
    echo "  ./tools/restart_historyserver.sh"
    echo ""
    echo "Or check running historyserver process logs"
fi
