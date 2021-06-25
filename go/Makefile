SHELL=/bin/bash

.PHONY:
all:
	make clean
	make isuumo

isuumo: *.go
	go build -o isuumo

.PHONY:
clean:
	-rm isuumo
