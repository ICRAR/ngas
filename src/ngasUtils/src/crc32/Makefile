
all: libchecksse42.so
	@echo Make done
	
libchecksse42.so: checksse42.o
	gcc -shared -Wl -o libchecksse42.so  checksse42.o
	
checksse42.o: checksse42.c
	gcc -c -fPIC checksse42.c -o checksse42.o

clean:
	@rm -f *.o *.so