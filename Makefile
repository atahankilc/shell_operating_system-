CC=g++
CFLAGS=-c -ansi -Wall -pedantic-errors -O0

string: main.cpp
	$(CC) main.cpp -o hw1

clean:
	rm hw1