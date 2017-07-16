# makefile
# This makes "dbloader" and "dbunloader"

CC=esql
#CC=c4gl

LN=ln
#LN=cp

all: dbloader dbunloader

dbloader: dbloader.ec
	$(CC) -O dbloader.ec -o dbloader -s
	@rm -f dbloader.c

dbunloader: dbloader
	$(LN) -f dbloader dbunloader
