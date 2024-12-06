#!/bin/bash

# 开始输出数组定义
echo "byte[] result = {"

first_line=true
while IFS= read -r line; do
  # 提取 | 和 | 之间的十六进制部分
  hex_section=$(echo "$line" | awk -F'|' '{print $2}' | tr -d ' ')
  
  if [[ -n "$hex_section" ]]; then
    # 转换为十六进制字节的形式：0xXX, 0xXX, ...
    java_bytes=$(echo "$hex_section" | sed -E 's/([0-9A-Fa-f]{2})/0x\1, /g')
    # 移除最后多余的逗号和空格
#    java_bytes=$(echo "$java_bytes" | sed 's/, $//')
    
    # 处理首行格式
    if $first_line; then
      echo -n "    $java_bytes"
      first_line=false
    else
      echo -n "    $java_bytes"
    fi
    echo
  fi
done

# 结束数组定义
echo "};"
