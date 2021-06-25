#!/bin/bash -eu

source "$HOME/.cargo/env"
# BEGIN ANSIBLE MANAGED BLOCK Rust
export PATH=/home/isucon/.cargo/bin:$PATH
# END ANSIBLE MANAGED BLOCK Rust
# BEGIN ANSIBLE MANAGED BLOCK deno
export PATH=/home/isucon/.deno/bin:$PATH
# END ANSIBLE MANAGED BLOCK deno
# BEGIN ANSIBLE MANAGED BLOCK php
export PATH=/home/isucon/local/php/bin:$PATH
# END ANSIBLE MANAGED BLOCK php
# BEGIN ANSIBLE MANAGED BLOCK Ruby
export PATH=/home/isucon/local/ruby/bin:$PATH
# END ANSIBLE MANAGED BLOCK Ruby
# BEGIN ANSIBLE MANAGED BLOCK perl
export PATH=/home/isucon/local/perl/bin:$PATH
# END ANSIBLE MANAGED BLOCK perl
# BEGIN ANSIBLE MANAGED BLOCK Node
export PATH=/home/isucon/local/node/bin:$PATH
# END ANSIBLE MANAGED BLOCK Node
# BEGIN ANSIBLE MANAGED BLOCK go
export PATH=/home/isucon/local/go/bin:/home/isucon/go/bin:$PATH
export GOROOT=/home/isucon/local/go
# END ANSIBLE MANAGED BLOCK go
# BEGIN ANSIBLE MANAGED BLOCK python
export PATH=/home/isucon/local/python/bin:$PATH
# END ANSIBLE MANAGED BLOCK python

### COPY

cd /home/isucon/isucon10-qualify-revenge
git pull
cd /home/isucon/isuumo/webapp/go
make all

sudo systemctl stop isuumo.go.service
sudo systemctl start isuumo.go.service
sudo systemctl status isuumo.go.service
sudo systemctl stop nginx.service
sudo systemctl start nginx.service
sudo systemctl status nginx.service