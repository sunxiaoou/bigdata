#!/bin/bash

# 配置参数

KEYSTORE_PATH="$KERB5_HOME/ca/keystore"
TRUSTSTORE_PATH="$KERB5_HOME/ca/truststore"
CERT_PATH="$KERB5_HOME/ca/hadoop.cer"
STOREPASS="maXiaoc1"
KEYPASS="maXiaoc1"
ALIAS="hadoop"
DNAME="CN=$HOSTNAME, OU=unit, O=org, L=cy, ST=bj, C=CN"
VALIDITY=3650

echo "📌 创建 keystore 并生成自签名证书..."
keytool -genkeypair \
  -alias $ALIAS \
  -keyalg RSA \
  -keysize 2048 \
  -dname "$DNAME" \
  -validity $VALIDITY \
  -keystore $KEYSTORE_PATH \
  -storepass $STOREPASS \
  -keypass $KEYPASS \
  -storetype JKS

echo "📌 从 keystore 导出证书..."
keytool -export \
  -alias $ALIAS \
  -keystore $KEYSTORE_PATH \
  -storepass $STOREPASS \
  -file $CERT_PATH

echo "📌 将证书导入到 truststore..."
keytool -import \
  -alias $ALIAS \
  -file $CERT_PATH \
  -keystore $TRUSTSTORE_PATH \
  -storepass $STOREPASS \
  -noprompt \
  -storetype JKS

echo "✅ 证书生成完成"
echo "📍 Keystore:     $KEYSTORE_PATH"
echo "📍 Truststore:   $TRUSTSTORE_PATH"
echo "📍 证书文件:     $CERT_PATH"
keytool -list -v -keystore $KERB5_HOME/ca/keystore -storepass $STOREPASS