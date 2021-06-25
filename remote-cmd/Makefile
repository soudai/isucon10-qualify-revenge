PHONY:
restart:
	sorce ~/.bashrc
	cd /home/isucon/isucon10-qualify-revenge && git pull
	cd /home/isucon/isuumo/webapp/go && make all
	sudo systemctl restart isuumo.go.service
	sudo systemctl status isuumo.go.service
	sudo systemctl restart nginx.service
	sudo systemctl status nginx.service


