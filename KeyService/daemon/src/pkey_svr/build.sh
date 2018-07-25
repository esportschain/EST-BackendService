export GOPATH=/root/golang/ && go build && rm -f /data/daemon/release/bin/pkey_svr && mv pkey_svr /data/daemon/release/bin && supervisorctl restart all
