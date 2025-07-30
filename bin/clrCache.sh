#!/bin/bash

# clrCache.sh - 清除指定UUID目录中的缓存文件
# 用法: ./clrCache.sh [UUID前4位]

if [ $# -eq 0 ]; then
    echo "错误：缺少UUID前缀参数"
    echo "用法: $0 <UUID前4位>"
    exit 1
fi

uuid_prefix="$1"
breakpoint_dir="breakpoint"
tf_dir="tf"

# 验证参数格式（只能包含十六进制字符）
if ! [[ "$uuid_prefix" =~ ^[0-9A-Fa-f]{4}$ ]]; then
    echo "错误：无效的UUID前缀 '$uuid_prefix'，必须是4位十六进制字符"
    exit 2
fi

# 转换前缀为大写格式（UUID通常是大写）
uuid_prefix_upper=$(echo "$uuid_prefix" | tr '[:lower:]' '[:upper:]')

# 在breakpoint目录中查找匹配的UUID目录（带连字符）
breakpoint_match="$breakpoint_dir/$uuid_prefix_upper"*

# 在tf目录中查找匹配的UUID目录（带下划线）
tf_match="$tf_dir/${uuid_prefix_upper}"*

# 查找实际存在的目录
found_dirs=()
shopt -s nullglob
for dir in $breakpoint_match $tf_match; do
    if [ -d "$dir" ]; then
        found_dirs+=("$dir")
    fi
done

if [ ${#found_dirs[@]} -eq 0 ]; then
    echo "未找到以 $uuid_prefix_upper 开头的UUID目录"
    exit 3
fi

# 显示找到的目录并确认
echo "找到以下目录:"
printf "  - %s\n" "${found_dirs[@]}"
read -p "确认要删除这些目录中的所有文件吗？[y/N] " confirm

if [[ ! "$confirm" =~ ^[Yy]$ ]]; then
    echo "操作已取消"
    exit 0
fi

# 删除目录中的所有文件（保留目录本身）
for dir in "${found_dirs[@]}"; do
    echo "正在清理: $dir"
    find "$dir" -mindepth 1 -maxdepth 1 -exec rm -rf {} +
    
    # 验证是否删除成功
    if [ $? -eq 0 ] && [ -z "$(ls -A "$dir" 2>/dev/null)" ]; then
        echo "  成功清理"
    else
        echo "  警告：可能有文件未被完全删除"
    fi
done

echo "清理操作完成"
