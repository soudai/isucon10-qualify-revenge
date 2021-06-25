#!/bin/bash

/home/isucon/isuumo/webapp/go/isuumo >> /home/isucon/isucon10-qualify-revenge/logs/app.log 2>&1

# $ dlv exec /home/isucon/isuumo/webapp/go/isuumo --headless --log --listen=127.0.0.1:22345 --api-version=2 --accept-multiclient --continue
