# pkey_svr
get privateKey Http server build by golang

## run
`./pkey_svr --port 8080 -ip 127.0.0.1
> HTTP Server listen`127.0.0.1:8080`

## HTTP service
`/get_pkey`

* method `POST`   
* request Body and return formate `json`

### request body
```json
{
    "tk": "08a6a8f98f2c6312089219c9b1eeda81",
    "sig": "hostname"
}

### return
```json
{
  "code": 0,
  "message": "success",
  "data": "key str"
}


## build project

```
$ export GOPATH=/data/daemon/golang/
$ cd /data/daemon/golang/src/pkey_svr
$ go get github.com/vmihailenco/msgpack
$ go build && rm -f /data/daemon/release/bin/pkey_svr && mv pkey_svr /data/daemon/release/bin
```

- Supervisor conf

```
[program:pkey_svr]

command=/data/daemon/release/bin/pkey_svr --port 8080 -ip 127.0.0.1
directory=/data/daemon/release/bin
user=www

# autorestart
autorestart=true
# logDir
stdout_logfile=/data/logs/supervisor/web_scraping/pkey_svr.log
loglevel=error
```