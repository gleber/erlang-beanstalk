SHELL=/bin/sh

EFLAGS=-pa ebin

ebins:
	test -d ebin || mkdir ebin
	erl $(EFLAGS) -make

clean:
	rm -rf ebin
