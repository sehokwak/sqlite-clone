db: db.c
	gcc -o db db.c

clean:
	-rm -f db