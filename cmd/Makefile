
PHONY:
restart:
	ssh i1 "cd /home/isucon/isucon10-qualify-revenge && git pull"
	ssh i1 "cd /home/isucon/isuumo/webapp/go && make isucon"
	ssh i1 sudo systemctl restart isuumo.go.service
	ssh i1 sudo systemctl status isuumo.go.service
	ssh i1 sudo systemctl restart nginx.service
	ssh i1 sudo systemctl status nginx.service

PHONY:
info:
	ssh i1 "cat /proc/cpuinfo | grep 'model name' | uniq -c" 
	ssh i1 "free -m"


