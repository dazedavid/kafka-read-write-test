#!/bin/sh
keytool -keystore kafka.keystore.jks -alias localhost -validity 3650 -keyalg RSA -genkey
openssl req -new -x509 -keyout ca-key -out ca-cert -days 3650
keytool -keystore kafka.truststore.jks -alias CARoot -import -file ca-cert
keytool -keystore kafka.truststore.jks -alias CARoot -import -file ca-cert

keytool -keystore kafka.keystore.jks -alias localhost -certreq -file cert-file
openssl x509 -req -CA ca-cert -CAkey ca-key -in cert-file -out cert-signed -days 3650 -CAcreateserial -passin pass: password
keytool -keystore kafka.keystore.jks -alias CARoot -import -file ca-cert
keytool -keystore kafka.keystore.jks -alias localhost -import -file cert-signed

keytool -importkeystore -srckeystore kafka.truststore.jks -destkeystore server.p12 -deststoretype PKCS12
openssl pkcs12 -in server.p12 -nokeys -out server.cer.pem
keytool -importkeystore -srckeystore kafka.keystore.jks -destkeystore client.p12 -deststoretype PKCS12
openssl pkcs12 -in client.p12 -nokeys -out client.cer.pem
openssl pkcs12 -in client.p12 -nodes -nocerts -out client.key.pem
