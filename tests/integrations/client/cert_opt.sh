#!/bin/bash
cert_dir="$2"

function generate_certs() {
    if [[ ! -z "$cert_dir" ]]; then
        cd "$cert_dir" || exit 255 # Change to the specified directory
    fi

    if ! [[ "$0" =~ "cert_opt.sh" ]]; then
        echo "must be run from 'cert'"
        exit 255
    fi

    if ! which openssl; then
        echo "openssl is not installed"
        exit 255
    fi

    # Generate CA private key and self-signed certificate
    openssl genpkey -algorithm RSA -out ca-key.pem
    openssl req -new -x509 -key ca-key.pem -out ca.pem -days 1 -subj "/CN=ca"
    # pd-server
    openssl genpkey -algorithm RSA -out pd-server-key.pem
    openssl req -new -key pd-server-key.pem -out pd-server.csr -subj "/CN=pd-server"

    # Add IP address as a SAN
    echo "subjectAltName = IP:127.0.0.1" >extfile.cnf
    openssl x509 -req -in pd-server.csr -CA ca.pem -CAkey ca-key.pem -CAcreateserial -out pd-server.pem -days 1 -extfile extfile.cnf

    # Clean up the temporary extension file
    rm extfile.cnf

    # client
    openssl genpkey -algorithm RSA -out client-key.pem
    openssl req -new -key client-key.pem -out client.csr -subj "/CN=client"
    openssl x509 -req -in client.csr -CA ca.pem -CAkey ca-key.pem -CAcreateserial -out client.pem -days 1

    # client2
    openssl genpkey -algorithm RSA -out tidb-client-key.pem
    openssl req -new -key tidb-client-key.pem -out tidb-client.csr -subj "/CN=tidb"
    openssl x509 -req -in tidb-client.csr -CA ca.pem -CAkey ca-key.pem -CAcreateserial -out tidb-client.pem -days 1
}

function cleanup_certs() {
    if [[ ! -z "$cert_dir" ]]; then
        cd "$cert_dir" || exit 255 # Change to the specified directory
    fi

    rm -f ca.pem ca-key.pem ca.srl
    rm -f pd-server.pem pd-server-key.pem pd-server.csr
    rm -f client.pem client-key.pem client.csr
    rm -f tidb-client.pem tidb-client-key.pem tidb-client.csr
}

if [[ "$1" == "generate" ]]; then
    generate_certs
elif [[ "$1" == "cleanup" ]]; then
    cleanup_certs
else
    echo "Usage: $0 [generate|cleanup] <cert_directory>"
fi
