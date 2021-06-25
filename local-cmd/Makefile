SHELL=/bin/bash

.PHONY:
restart:
	ssh i1 "export; which go"

.PHONY:
info:
	ssh i1 "cat /proc/cpuinfo | grep 'model name' | uniq -c"
	ssh i1 "free -m"
