.PHONY:
restart:
	ssh i1 "bash -c \"source ~/.bashrc && cd /home/isucon/isucon10-qualify-revenge/remote-cmd && make restart \""

PHONY:
info:
	ssh i1 "cat /proc/cpuinfo | grep 'model name' | uniq -c"
	ssh i1 "free -m"
