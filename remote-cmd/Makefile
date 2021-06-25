SHELL=/bin/bash

PHONY:
restart:
	source ~/.bashrc
	cd /home/isucon/isucon10-qualify-revenge && git pull
	cd /home/isucon/isuumo/webapp/go && make all
	-sudo systemctl stop isuumo.go.service
	sudo systemctl start isuumo.go.service
	sudo systemctl status isuumo.go.service
	-sudo systemctl stop nginx.service
	sudo systemctl start nginx.service
	sudo systemctl status nginx.service


