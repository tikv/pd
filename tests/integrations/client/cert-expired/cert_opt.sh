#!/bin/bash
function generate_certs() {
    if ! [[ "$0" =~ "./cert_opt.sh" ]]; then
        echo "must be run from 'cert-expired'"
        exit 255
    fi

    if ! which cfssl; then
        echo "cfssl is not installed"
        exit 255
    fi

    cfssl gencert -initca ca-csr.json | cfssljson -bare ca -

    # pd-server
    echo '{"CN":"pd-server","hosts":[""],"key":{"algo":"rsa","size":2048}}' | cfssl gencert -ca=ca.pem -ca-key=ca-key.pem -config=ca-config.json -profile=server -hostname="localhost,127.0.0.1" - | cfssljson -bare pd-server

    # client
    echo '{"CN":"client","hosts":[""],"key":{"algo":"rsa","size":2048}}' | cfssl gencert -ca=ca.pem -ca-key=ca-key.pem -config=ca-config.json -profile=client -hostname="" - | cfssljson -bare client
}

function cleanup_certs() {
    rm -f ca.pem ca-key.pem ca.csr
    rm -f pd-server.pem pd-server-key.pem pd-server.csr
    rm -f client.pem client-key.pem client.csr
}

if [[ "$1" == "generate" ]]; then
    generate_certs
elif [[ "$1" == "cleanup" ]]; then
    cleanup_certs
else
    echo "Usage: $0 [generate|cleanup]"
fi
