SOURCE_FILES := $(wildcard src/beanstalk*.erl)


all: gen_server2 $(SOURCE_FILES:src/%.erl=ebin/%.beam)

gen_server2:
	@test -d ebin || mkdir ebin
	erlc -W +debug_info -o ebin src/gen_server2.erl

ebin/%.beam: src/%.erl
	@test -d ebin || mkdir ebin
	erlc -pa ebin -W +debug_info -o ebin $<

clean:
	rm -rf ebin
