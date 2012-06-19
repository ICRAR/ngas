#include "Python.h"
#include <numarray/arrayobject.h>
#include "fitsio.h"
#include "stdio.h"
#include "stdlib.h"
#include "string.h"
#include "math.h"

fitsfile *nfopen(const char *filename, int iomode, int *status)
{
	fitsfile *tmp;
       	/* int status=1; */

	ffopen(&tmp, filename, iomode, status);
	if (!status) {
	  fprintf(stderr,"Could not open file %s\n",filename);
	}
	return  tmp;
}


fitsfile *nfreopen(fitsfile *fptr)
{
	fitsfile *tmp;
	int status=0;

	ffreopen(fptr, &tmp, &status);

	return  tmp;
}

fitsfile *nfinit(char *filename, int *status)
{
	fitsfile *tmp;
	/*	int status=0;*/
	char str[255];

	ffinit(&tmp, filename, status);
	//if (status) {
	//  fprintf(stderr,"Could not open file %s\n",filename);
	//}
	return  tmp;
}

fitsfile *nftplt(char *filename, char *tpltfile)
{
	fitsfile *tmp;
	int status=0;

	fftplt(&tmp,filename, tpltfile, &status);
	if (status) {
	  fprintf(stderr,"Could not open file %s\n",filename);
	}
	return  tmp;
}

fitsfile *nffgtop(fitsfile *fptr, int group)
{
         fitsfile *tmp;
	 int status = 0;
	 ffgtop(fptr,group,&tmp,&status);
	 return tmp;
}

fitsfile *nffgmop(fitsfile *gfptr, long member)
{
         fitsfile *tmp;
	 int status = 0;
	 ffgmop(gfptr,member,&tmp,&status);
	 return tmp;
}
