#!/bin/bash
# Enable TLS on the FE's MySQL port, then hand off to the image's entrypoint.
# The OpenID client plugin refuses to send its token over plaintext.
set -euo pipefail

FE_CONF=/data/deploy/starrocks/fe/conf
KEYSTORE=$FE_CONF/fe.p12

if [ ! -f "$KEYSTORE" ]; then
    keytool -genkeypair -alias starrocks -keyalg RSA -keysize 2048 -validity 30 \
        -dname CN=starrocks -ext SAN=dns:starrocks,dns:localhost,ip:127.0.0.1 \
        -storetype PKCS12 -keystore "$KEYSTORE" -storepass changeit -keypass changeit
    cat >> "$FE_CONF/fe.conf" <<EOF

ssl_keystore_location = $KEYSTORE
ssl_keystore_password = changeit
ssl_key_password = changeit
ssl_keystore_type = PKCS12
EOF
fi

# Publish the self-signed certificate so the test can verify the FE with it.
keytool -exportcert -rfc -alias starrocks -keystore "$KEYSTORE" -storepass changeit \
    -file /certs/starrocks.pem

cp /openid/dbt_groups.txt "$FE_CONF/dbt_groups.txt"

exec ./entrypoint.sh
