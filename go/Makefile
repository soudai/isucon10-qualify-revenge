SHELL=/bin/bash

.PHONY:
all:
	make clean
	make isuumo

isuumo: *.go
	go build -o isuumo

.PHONY:
debug:
	make clean
	go build -gcflags \"all=-N\" -o isuumo

.PHONY:
clean:
	-rm isuumo
