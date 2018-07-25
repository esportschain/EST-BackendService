package main

import (
    "pkey_svr/cmd"
)

func main()  {
    cmd := new(cmd.Cmd)
    cmd.Run()
}
