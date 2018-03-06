server:server.o kv.o parser.o
	gcc -o server server.o kv.o parser.o -lpthread

server.o: server.c kv.h parser.h
	gcc -c server.c

kv.o: kv.c kv.h
	gcc -c kv.c

parser.o: parser.c parser.h
	gcc -c parser.c

clean:
	rm server parser.o server.o kv.o