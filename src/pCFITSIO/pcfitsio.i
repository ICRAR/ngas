%module pcfitsio

%{
#include "fitsio.h"
#include "pcfitsio.h"
%}
%include typemaps.i
%include type.i


/* --------------- Handling of special functions dealing with **fitsfile --------------*/
%rename nfopen fits_open_file;
%rename nfreopen fits_reopen_file;
%rename nfinit fits_create_file;
%rename nftplt fits_create_template;
%rename nffgtop fits_open_group;
%rename nffgmop fits_open_member;

%apply char *input {char *filename};
%apply int input {input mode};
fitsfile *nfopen(const char *filename,  int mode, int *status);
/* Returns a FITSfile pointer to an existing FITS file */
/* Set mode = 0 for READONLY */
/* Set mode = 1 for READWRITE */

fitsfile *nfreopen(fitsfile *fptr);
/* Returns a new FITSfile pointer to an already opened */
/* FITS file */

fitsfile *nfinit(char *filename, int *status);
/* Initializes a new FITS file with name filename */
/* and returns a FITSfile pointer */
/* No header nor extension is created */

fitsfile *nftplt(char *filename, char *template);
/* Returns a new FITSfile pointer after creating a new */
/* FITS file using a template FITS file */

fitsfile *nffgtop(fitsfile *fptr, int group);
/* Returns a fitsfile pointer pointing to the opened grouping table */

fitsfile *nffgmop(fitsfile *gfptr, long member);
/* Returns a fitsfile pointer pointing to the opened grouping table */



%apply fitsfile *delfitsfile {fitsfile *fptr};
void fits_close_file(fitsfile *fptr, int *status);
/* Close an already opened FITS file pointed to by the */
/* given FITSfile pointer */

void fits_delete_file(fitsfile *fptr, int *status);
/* Deletes the FITS file pointed to by the given */
/* FITSfile pointer */

%clear fitsfile *fptr;
void fits_file_name(fitsfile *fptr, char *output, int *status);
/* Returns the name of the FITS file pointed to by the given */
/* FITSfile pointer */

void fits_file_mode(fitsfile *fptr, int *output, int *status);
/* Returns the access mode  of the FITS file pointed to by the given */
/* FITSfile pointer */
/* = 0 for READONLY */
/* = 1 for READWRITE */

/*------------------ checksum function -------------------*/

/*------------------ get header information --------------*/
%apply int *output {int *nexist};
%apply int *output {int *nmore};
%apply int *output {int *position};
void fits_get_hdrspace(fitsfile *fptr, int *nexist, int *nmore, int *status);
/* Returns a PyList containing the number of existing keywords and */
/* the number of free cards in the current FITS extension */

void fits_get_hdrpos(fitsfile *fptr, int *nexist, int *position, int *status);
%clear int *nextist;
%clear int *nmore;
%clear int *position;
/* Returns a PyList containing the number of cards in the current */
/* header and the current position in the current FITS extension's header */

%apply char *output {char *keyname};
%apply char *output {char *keyval};
%apply char *output {char *comment};
%apply int input {int nkey};
/*void fits_read_keyn(fitsfile *fptr, int nkey, char *keyname, char *keyval, char *comment, *status);*/
%clear char *keyname;

%apply char *input {char *keyname};
%apply int *output {int *value};
%apply double *output {double *value};
%apply long *output {long *value};
void fits_read_keyword(fitsfile *fptr, char *keyname, char *keyval, char *comment, int *status);
void fits_read_key_log(fitsfile *fptr, char *keyname, int *value, char *comment, int *status);

void fits_read_key_dbl(fitsfile *fptr, char *keyname, double *value,char *comment,int *status);
void fits_read_key_lng(fitsfile *fptr, char *keyname, long *value, char *comment, int *status);
%clear char *keyname;
%clear int *value;
%clear long *value;
%clear double *value;
%clear char *comment;

/*------------------ move position in header -------------*/
%apply int input {int nrec};
void fits_movabs_key(fitsfile *fptr, int nrec, int *status);
/* Move to the absolute nrec card in the current extension's header */

void fits_movrel_key(fitsfile *fptr, int nrec, int *status);
/* Move to the nrec card in the current extension's header */
/* relativley to the current position in that header */
%clear int nrec;

/*---------------- utility routines -------------*/
%apply float *output {float *version};
float fits_get_version(float *version);
/* Returns the version of the CFITSIO module*/
%clear float *version;
%apply char *output {char *errtext};
void fits_get_errstatus(int input, char *errtext);
/* Returns the text description corresponding to the given CFITSIO */
/* error */
%clear char *errtext;
%apply char *input {char *err_message};
void fits_write_errmsg(const char *err_message);
/* Write the given error message into the CFITSIO error stack */
%clear char *err_message;
%apply char *output {char *err_message};
void fits_read_errmsg(char *err_message);
/* Returns a list containg the latest integer error and */
/* the corresponding text message from the CFITSIO error stack */ 
%clear char *err_message;
void fits_clear_errmsg(void);
/* Clears the CFITSIO error message stack */

/*--------------------- update keywords ---------------*/
%apply char *input {char *keyname};
%apply void *input {void *value};
%apply char *input {char *comment};

void fits_update_key(fitsfile *fptr, int voidtype, char *keyname, void *value, char *comment, int *status);
void fits_update_key_null(fitsfile *fptr, char *keyname, char *comm, int *status);
/* Updates a key/comment pair in the current extension's header */
/* Replaces with the given values of key and comment */
%clear char *keyname;
%clear void *value;
%clear char *comment;

/*----------------- write single keywords --------------*/
%apply char *input {char *comment};
void fits_write_comment(fitsfile *fptr, char *comment, int *status);
/* Writes a COMMENT card in the current extension's header */
%apply char *input {char *hist};
void fits_write_history(fitsfile *fptr, char *hist, int *status);
/* Writes a HISTORY card in the current extension's header */
void fits_write_date(fitsfile *fptr, int *status);
/* Writes/updates DATE card with the current date and time (UTC) */
/* in the current extension's header */

%apply char *input {char *card};
void fits_write_record(fitsfile *fptr, char *card, int *status);
/* Writes a complete card into the current extension's header */
%apply char *input {char *keyname};
%apply char *input {char *unit};
void fits_write_key_unit(fitsfile *fptr, char *keyname, char *unit, int *status);
/* Writes/updates the units in [] for the given key */
%clear char *comment;
%clear char *hist;
%clear char *card;
%clear char *keyname;
%clear char *unit;
/*------------------ read single keywords -----------------*/
%apply int input {int nrec};
%apply char *output {char *card};
void fits_read_record(fitsfile *fptr,  int nrec, char *card, int *status);
/* Reads and return the nth record (string) of the current extension's header */
%apply char *input {char *keyname};
void fits_read_card(fitsfile *fptr, char *keyname, char *card, int *status);
/* Reads and return the record (string) corresponding to the given key */
%apply char *output {char *unit};
char* fits_read_key_unit(fitsfile *fptr, char *keyname, char *unit, int  *status);
/* Returns the units of the given key */
%clear int nrec;
%clear char *card;
%clear char *keyname;

/*--------------------- update keywords ---------------*/
%apply char *input {char *keyname};
%apply void *input {void *value};
%apply char *input {char *comment};
void fits_write_key(fitsfile *fptr, int voidtype, char *keyname, void *value,
          char *comment, int *status);
/* Adds a new key to the current extension's header */
/* The given value is assigned to the key. Integer, double, and string */
/* casting is done automatically. */
/* Also writes the given comment which can be an empty string */
%apply char *input {char *card};

void fits_update_card(fitsfile *fptr, char *keyname, char *card, int *status);
/* Replaces the given keyword by the given card string */
/* replacing the given ley too. No checking is performed in the */
/*  given card */

%apply char *input {char *oldname};
%apply char *input {char *newname};
void fits_modify_name(fitsfile *fptr, char *oldname, char *newname, int *status);
/* Modifies the name of a key to a new name */
void fits_modify_comment(fitsfile *fptr, char *keyname, char *comment, int *status);
/* Modifies the comment of the given key */
void fits_modify_key_null(fitsfile *fptr, char *keyname, char *comment, int *status);
/* Assigns the NULL/comment pair to the given key */
%apply char *input {char *value};
void fits_modify_key_str(fitsfile *fptr, char *keyname, char *value, char *comment,int *status);
/* Replaces the value/comment pair of the given key with */
/* the given string/comment pair */
%apply int *input {int *value};
void fits_modify_key_log(fitsfile *fptr, char *keyname, int value, char *comment, int *status);
/* Replaces the value/comment pair of the given key with */
/* the given boolean/comment pair */
%apply long *input {long *value};
void fits_modify_key_lng(fitsfile *fptr, char *keyname, long value, char *comment, int *status);
/* Replaces the value/comment pair of the given key with */
/* the given long/comment pair */
%clear char *keynam;
%clear void *value;
%clear char *comment;
%clear char *card;
%clear char *oldname;
%clear char *newname;
%clear char *value;
%clear int *value;
%clear long *value;

/*--------------------- long string support ------------*/
%apply char *input {char *keyname};
%apply char *input {char *longstr};
%apply char *input {char *comment};

void fits_write_key_longstr(fitsfile *fptr, char *keyname, char *longstr, char *comment,int *status);
%clear char *keyname;
%clear char *longstr;
%clear char *comment;

/*--------------------- delete keywords ---------------*/
%apply char *input {char *keyname};
%apply int input {int keypos};
void fits_delete_key(fitsfile *fptr, char *keyname, int *status);
/* Deletes the given key (by name) from the current extension's header */
void fits_delete_record(fitsfile *fptr, int keypos, int *status);
/* Deletes the given key (by position) from the current extension's header */
%clear char *keyname;
%clear int keypos;

/*--------------------- get HDU information -------------*/
%apply int *output {int *chdunum};
%apply int *output {int *exttype};
void fits_get_hdu_num(fitsfile *fptr, int *chdunum);
/* Returns the number of extension in this FITS file */
void fits_get_hdu_type(fitsfile *fptr, int *exttype, int *status);
/* Returns the type of the current extension */
%apply long *output {long *headstart, long *datastart, long *dataend};
void fits_get_hduaddr(fitsfile *fptr, long *headstart, long *datastart, long *dataend, int *status);
/* Returns a PyList containing the beginning address and the ending address */
/* of the current extenstion's header, and the total size of this FITS */
/* file. */
%clear int *chdunum, int *exttype, long *headstart, long *datastart, long *dataen;

/*--------------------- HDU operations -------------*/
%apply int input {int hdunum};
%apply int *output {int *exttype};
void fits_movabs_hdu(fitsfile *fptr, int hdunum, int *exttype, int *status);
/* Moves to a given (absolute number) extension in the FITS file */
/* In this case, the first extension is numbered 1 */
%clear int hdunum,int *exttype;

%apply int input {int hdumov};
%apply int *output {int *exttype};
void fits_movrel_hdu(fitsfile *fptr, int hdumov, int *exttype, int *status);
/* Moves to a given (relatively to current position) extension in the FITS file */
/* i.e. +1 moves 1 forward, -2 moves backward 2 */
%clear int hdumov,int *exttype;

%apply int input {int exttype};
%apply char *input {char *hduname};
%apply int input {int hduvers};
void fits_movnam_hdu(fitsfile *fptr, int exttype, char *hduname, int hduvers,
           int *status);
/* Moves to the given (by name) extension */
%clear int exttype,char *hduname,int hduvers;

%apply int *output {int *nhdu};
void fits_get_num_hdus(fitsfile *fptr, int *nhdu, int *status);
/* Returns the number of extensions in the FITS file */
%clear int *nhdu;

void fits_create_hdu(fitsfile *fptr, int *status);

%apply int input {int bitpix};
%apply int input_array_size {int naxis};
%apply long *input_listarray {long *naxes};
void fits_create_img(fitsfile *fptr, int bitpix, int naxis, long *naxes, int *status);
/* Creates (appends to file ) an image extension, using the given bitpix value, and naxis */
/* values contained in the array naxes */
%clear int bitpix,int naxis,long *naxes;

%apply int *output {int *hdutype};
void fits_delete_hdu(fitsfile *fptr, int *hdutype, int *status);
/* Deletes the current extension from the FITS file */
/* Returns the type of the new current extension */
%clear int *hdutype;

%apply int input {int morekeys};
void fits_copy_hdu(fitsfile *infptr, fitsfile *outfptr, int morekeys, int *status);
/* Copy the current extension associated with infptr to the end of the file associated with outfptr */
/* Space for a number of additional keys can be reserved by setting morekeys */
/* to a non-zero value */
%clear int morekeys;

/*------------ write primary array or image elements -------------*/
%apply long  input {long  firstelem, long  nelem};
%apply void *input_numarray {void  *array};
void fits_write_img(fitsfile *fptr, int voidtype, long  firstelem, long  nelem,
          void  *array, int  *status);
/* Writes elements into the current image extension */
%clear long  firstelem, long  nelem, void  *array;
%apply int input {int bitpix};
%apply int input_array_size {int naxis};
%apply long *input_listarray {long *naxes};
int fits_resize_img(fitsfile *fptr, int bitpix, int naxis, long *naxes, int *status);
%clear int bitpix, int naxis, long *naxes;


/*--------------------- read primary array or image elements -------------*/
%apply long input {long  firstelem};
%apply void *input {void *nulval};
%apply void *output_numarray {void *array};
%apply int *output {int *anynul};
void fits_read_img(fitsfile *fptr, int  voidtype, long firstelem, long nelements,
          void *nulval, void *array, int *anynul, int  *status);
/* Reads nelements, starting at position firstelem, from the current image */
/* extension. Substitute the given nulval value for any null values */
/* in the image. Returns an array containing the flattened image as first */
/* element, and the number of null value in the second element */
%clear long  firstelem, long  nelements,void *nulval,void *array,int *anynul;

/*----------------- write required header keywords --------------*/
%apply int input {int bitpix};
%apply int input_array_size {int naxis};
%apply long *input_listarray {long *naxes};
void fits_write_imghdr(fitsfile *fptr, int bitpix, int naxis, long naxes[], int *status);
/* Writes the BITPIX, and NAXES? keywords in the current extension's header */
%clear int bitpix,int naxis,long *naxes;

/*--------------------- get column information -------------*/
%apply int *output {int *ncols};
%apply long *output {long *nrows};
void fits_get_num_cols(fitsfile *fptr, int  *ncols, int *status);
void fits_get_num_rows(fitsfile *fptr, long *nrows, int *status);
%clear int *ncols, long *nrows;

/*--------------------- get column information -------------*/
%apply int input {int casesen};
%apply char *input {char *templt};
%apply int *output {int *colnum};
%apply char *output {char *colname};
int fits_get_colnum(fitsfile *fptr, int casesen, char *templt, int *colnum,
           int *status);
int fits_get_colname(fitsfile *fptr, int casesen, char *templt, char *colname,
           int *colnum, int *status);
%clear int casesen,char *templt,int *colnum,char *colname;
%apply int input {int colnum};
%apply int *output {int *typecode};
%apply long *output {long *repeat, long *width};
void fits_get_coltype(fitsfile *fptr, int colnum, int *typecode, long *repeat,
           long *width, int *status);
%clear int colnum,int *typecode,long *repeat, long *width;


/*--------------------- read column elements -------------*/
%apply int input {int colnum};
%apply long input {long firstrow, long firstelem};
%apply int *output {int *anynul};


%apply char *output_listarray{char *larray};
int fits_read_col_bit(fitsfile *fptr, int colnum, long firstrow, long firstelem,
            long nelements, char *larray, int *status);
%clear char *larray;

%apply void *output_listarray{void *array};
%apply void *input {void *nulval};
void fits_read_col( fitsfile *fptr, int voidtype, int colnum, long firstrow,
           long firstelem, long nelements, void *nulval, void *array, int *anynul,
           int  *status);
%clear void *array,void *nulval;

%apply unsigned char *output_listarray{unsigned char *array};
%apply unsigned char input {short nulval};
void fits_read_col_byt(fitsfile *fptr, int colnum, long firstrow, long firstelem,
           long nelements, unsigned char nulval,  unsigned char *array, 
           int *anynul, int *status);
%clear unsigned char *array, unsigned char nulval;

%apply char *output_listarray{char *array};
%apply char input {short nulval};
void fits_read_col_log(fitsfile *fptr, int colnum, long firstrow, long firstelem,
           long nelements, char nulval,  char *array, 
           int *anynul, int *status);
%clear char *array, char nulval;

%apply short *output_listarray{short *array};
%apply short input {short nulval};
void fits_read_col_sht(fitsfile *fptr, int colnum, long firstrow, long firstelem,
           long nelements, short nulval,  short *array, 
           int *anynul, int *status);
%clear short *array,short nulval;

%apply unsigned short *output_listarray{unsigned short *array};
%apply unsigned short input {unsigned short nulval};
void fits_read_col_usht(fitsfile *fptr, int colnum, long firstrow, long firstelem,
           long nelements, unsigned short nulval,  unsigned short *array, 
           int *anynul, int *status);
%clear unsigned short *array,unsigned short nulval;


%apply int *output_listarray{int *array};
%apply int input {int nulval};
void fits_read_col_int(fitsfile *fptr, int colnum, long firstrow, long firstelem,
           long nelements, int nulval,  int *array, 
           int *anynul, int *status);
%clear int *array,int nulval;

%apply unsigned int *output_listarray{unsigned int *array};
%apply unsigned int input {unsigned int nulval};
void fits_read_col_uint(fitsfile *fptr, int colnum, long firstrow, long firstelem,
           long nelements, unsigned int nulval,  unsigned int *array, 
           int *anynul, int *status);
%clear unsigned int *array,unsigned int nulval;

%apply long *output_listarray{long *array};
%apply long input {long nulval};
void fits_read_col_lng(fitsfile *fptr, int colnum, long firstrow, long firstelem,
           long nelements, long nulval,  long *array, 
           int *anynul, int *status);
%clear long *array,long nulval;

%apply unsigned long *output_listarray{unsigned long *array};
%apply unsigned long input {unsigned long nulval};
void fits_read_col_ulng(fitsfile *fptr, int colnum, long firstrow, long firstelem,
           long nelements, unsigned long nulval,  unsigned long *array, 
           int *anynul, int *status);
%clear unsigned long *array,unsigned long nulval;

%apply float *output_listarray {float *array};
%apply float input {float nulval};
%apply int input {int colnum};
%apply long input {long firstrow, long firstelem, long nelements};
%apply int *output {int *anynul};
void fits_read_col_flt(fitsfile *fptr, int colnum, long firstrow, long firstelem,
           long nelements, float nulval,  float *array, 
           int *anynul, int *status);
%clear float *array,float nulval;

%apply double *output_listarray{double *array};
%apply double input {double nulval};
void fits_read_col_dbl(fitsfile *fptr, int colnum, long firstrow, long firstelem,
           long nelements, double nulval,  double *array, 
           int *anynul, int *status);
%clear double *array,double nulval;


%apply complex *output_listarray{float *array};
%apply float input {float nulval};
void fits_read_col_cmp(fitsfile *fptr, int colnum, long firstrow, long firstelem,
           long nelements, float nulval,  float *array, 
           int *anynul, int *status);
%clear float *array,float nulval;

%apply string *output_listarray{char **array};
%apply singlechar *input {char *nulval};
void fits_read_col_str(fitsfile *fptr, int colnum, long firstrow, long firstelem,
           long nelements, char *nulval, char **array, int *anynul, int *status);
%clear char **array, char *nulval;

%clear int colnum,long firstrow, long firstelem,int *anynul;


%apply int input {int tbltype};
%apply long input {long naxis2};
%apply long input_array_size {int tfields};
%apply string *input_listarray {char *ttype[], char *tform[], char *tunit[]};
%apply char *input {char *extname};
void fits_create_tbl(fitsfile *fptr, int tbltype, long naxis2, int tfields, char *ttype[],
       char *tform[], char *tunit[], char *extname, int *status);
%clear int tbltype, long naxis2, int tfiels, char *ttype[], char *tform[], char *tunits[];


/*--------------------- write column elements -------------*/
%apply int input {int colnum};
%apply long input {long firstrow, long firstelem, long nelem};
%apply long input_array_size {long nelems};


int fits_write_col_null(fitsfile *fptr, int colnum, long firstrow, long firstelem,
           long nelems, int *status);

%apply char *input_listarray {char *larray};
int fits_write_col_bit(fitsfile *fptr, int colnum, long firstrow, long firstelem , long nelems,
            char *larray, int *status);
%clear char *larray;

%apply void *input_listarray {void *array};
int fits_write_col(fitsfile *fptr, int voidtype, int colnum, long firstrow,
          long firstelem, long nelems, void *array, int *status);
%clear void *array;

%apply unsigned char *input_listarray {unsigned char *array};
int fits_write_col_byt(fitsfile *fptr, int colnum, long firstrow, long firstelem,
           long nelems, unsigned char *array, int *status);
%clear unsigned char *array;

%apply char *input_listarray {char *array};
int fits_write_col_log(fitsfile *fptr, int colnum, long firstrow, long firstelem,
           long nelems, char *array, int *status);
%clear char *array;


%apply short *input_listarray {short *array};
int fits_write_col_sht(fitsfile *fptr, int colnum, long firstrow,
          long firstelem, long nelems, short *array, int *status);
%clear short *array;

%apply unsigned short *input_listarray {ushort *array};
int fits_write_col_usht(fitsfile *fptr, int colnum, long firstrow,
          long firstelem, long nelems, ushort *array, int *status);
%clear ushort *array;

%apply  long *input_listarray {long *array};
int fits_write_col_lng(fitsfile *fptr, int colnum, long firstrow,
          long firstelem, long nelems, long *array, int *status);
%clear long *array;

%apply  unsigned long *input_listarray {unsigned long *array};
int fits_write_col_ulng(fitsfile *fptr, int colnum, long firstrow,
          long firstelem, long nelems, unsigned long *array, int *status);
%clear unsigned long *array;

%apply  int *input_listarray {int *array};
int fits_write_col_int(fitsfile *fptr, int colnum, long firstrow,
          long firstelem, long nelems, int *array, int *status);
%clear int *array;

%apply  float *input_listarray {float *array};
int fits_write_col_flt(fitsfile *fptr, int colnum, long firstrow,
          long firstelem, long nelems, float *array, int *status);
%clear float *array;

%apply  double *input_listarray {double *array};
int fits_write_col_dbl(fitsfile *fptr, int colnum, long firstrow,
          long firstelem, long nelems, double *array, int *status);
%clear double *array;


%apply  string *input_listarray {char **array};
fits_write_col_str(fitsfile *fptr, int colnum, long firstrow, long firstelem,
           long nelems, char **array, int *status);


%clear int colnum,long firstrow, long firstelem, long nelem;
/*-----------------------------------------------------------*/



%apply long input {long firstrow, long nrows};
void fits_insert_rows(fitsfile *fptr, long firstrow, long nrows, int *status);
void fits_delete_rows(fitsfile *fptr, long firstrow, long nrows, int *status);
%clear long firstrow, long nrows,long *rownum;
%apply long *input_listarray {long *rownum};
%apply long input_array_size {long nrows};
void fits_delete_rowlist(fitsfile *fptr, long *rownum,  long nrows, int *status);
%clear long *rownum,long nrows;
%apply int input {int numcol};
void fits_delete_col(fitsfile *fptr, int numcol, int *status);
%clear int numcol;
%apply int input {int incol, int outcol, int create_col};
void fits_copy_col(fitsfile *infptr, fitsfile *outfptr, int incol, int outcol, 
           int create_col, int *status);
%clear int incol, int outcol, int create_col;
%apply int input {int numcol};
%apply char *input {char *ttype, char *tform};
void fits_insert_col(fitsfile *fptr, int numcol, char *ttype, char *tform, int *status);

%apply char *input {char *keyname};
%apply int *input {int nstart};
%apply int out_array_size {int nmax};
%apply long *out_array {long *value};
%apply int *out_array_nfound {int *nfound};
void fits_read_keys_lng(fitsfile *fptr, char *keyname, int nstart, int nmax, long *value,
           int *nfound, int *status);
%clear long *value;
%apply double *out_array {double *value};
void fits_read_keys_dbl(fitsfile *fptr, char *keyname, int nstart, int nmax, double *value,
           int *nfound, int *status);
%apply char *out_array[] {char *value[]};
void fits_read_keys_str(fitsfile *fptr, char *keyname, int nstart, int nmax,  char *value[],int *nfound, int *status);

%clear char *keyname,int nstart,int nmax,long *value,int *nfound,double *value;
/*--------------------- General utility functions -------------*/
%apply char *input {char *value};
%apply char *output {char *dtype};
void fits_get_keytype(char *value, char *dtype,int *status);
%clear char *value, char *dtype;
%apply long *output {unsigned long *datasum};
%apply long *output {unsigned long *hdusum};
int fits_get_chksum(fitsfile *fptr, unsigned long *datasum, unsigned long *hdusum, int *status);
%clear unsigned long *datasum,unsigned long *hdusum;
/*--------------------- WCS utils -----------------------------*/
%apply double *output {double *xrval};
%apply double *output {double *yrval};
%apply double *output {double *xrpix};
%apply double *output {double *yrpix};
%apply double *output {double *xinc};
%apply double *output {double *yinc};
%apply double *output {double *rot};
%apply char *input {char *type};
int fits_read_img_coord(fitsfile *fptr, double *xrval, double *yrval, double *xrpix, double *yrpix, double *xinc, double *yinc, double *rot,char *type, int *status);


/*--------------------- Group HDU functions ----------------*/
%apply char *input {char *grpname};
%apply int input {int grouptype};
int fits_create_group(fitsfile *fptr, char *grpname, int grouptype, int *status);
int fits_insert_group(fitsfile *fptr, char *grpname, int grouptype, int *status);
int fits_change_group(fitsfile *gfptr, int grouptype, int *status);

%apply int input {int rmopt};
int fits_remove_group(fitsfile *gfptr, int rmopt, int *status);

%apply int input {int cpopt};
int fits_copy_group(fitsfile *infptr, fitsfile *outfptr, int cpopt, int *status);

%apply int input {int mgopt};
int fits_merge_groups(fitsfile *infptr, fitsfile *outfptr, int mgopt, int *status);

%apply int input {int cmopt};
int fits_compact_group(fitsfile *gfptr, int cmopt, int *status);


%apply long *output {long *firstfailed};
int fits_verify_group(fitsfile *gfptr, long *firstfailed, int *status);


/*int ffgtop(fitsfile *mfptr,int group,fitsfile **gfptr,int *status);*/

%apply int input {int hdupos};
%apply fitsfile2 input {fitsfile *mfptr};
int fits_add_group_member(fitsfile *gfptr, fitsfile *mfptr, int hdupos, int *status);
%clear fitsfile *mfptr;

%apply long *output {long *nmembers};
int fits_get_num_members(fitsfile *gfptr, long *nmembers, int *status);
int fits_get_num_groups(fitsfile *mfptr, long *nmembers, int *status);

%apply int input {int member};
/* int ffgmop(fitsfile *gfptr, long member, fitsfile **mfptr, int *status);*/

%apply int input {int cpopt};
int fits_copy_member(fitsfile *gfptr, fitsfile *mfptr, long member, int cpopt, int *status);

%apply int input {int tfopt};
int fits_transfer_member(fitsfile *infptr, fitsfile *outfptr, long member, int tfopt, int *status);

%apply int input {int rmopt};
int fits_remove_member(fitsfile *fptr, long member, int rmopt, int *status);

%apply long input {long group};
%apply long input {long firstelem};
%apply long input {long nelem};
%apply long *input_array {long *array};
int fits_write_grppar_lng(fitsfile *fptr, long group, long firstelem,
           long nelem, long *array, int *status);

%apply double *input_array {double *array};
int fits_write_grppar_dbl(fitsfile *fptr, long group, long firstelem,
           long nelem, double *array, int *status);

%apply long *output_listarray{long *array};
%apply long nelements{long nelem};
int fits_read_grppar_lng(fitsfile *fptr, long group, long firstelem, long nelem,
           long *array, int *status);

%apply double *output_listarray{double *array};
%apply long nelements{long nelem};
int fits_read_grppar_dbl(fitsfile *fptr, long group, long firstelem, long nelem,
           double *array, int *status);
 

/*%apply int input {int colnum};
%apply int *output {int *typecode};
%apply long *output {long *repeat, long *width};
int fits_get_coltype(fitsfile *fptr, int colnum, int *typecode, long *repeat,
		     long *width, int *status);
*/

%apply int input {int colnum};
%apply int *output {int *width};
int fits_get_col_display_width(fitsfile *fptr, int colnum, int *width, int *status);

%apply int input {int colnum};
%apply char *output {char *ttype, char *tunits, char *tforms, char *tnull, char *tdisp};
%apply long *output {long *tbcol};
%apply double *output {double *tscal, double *tzero};
int fits_get_acolparms(fitsfile *fptr, int colnum, char *ttype, long *tbcol,
           char *tunit, char *tform, double *tscal, double *tzero,
           char *tnull, char *tdisp, int *status);


%apply int input {int colnum};
%apply char *output {char *ttype, char *tunits, char *dtype, char *tdisp};
%apply long *output {long *repeat, long *tnull};
%apply double *output {double *tscal, double *tzero};
int fits_get_bcolparms(fitsfile *fptr, int colnum, char *ttype, char *tunit,
           char *dtype, long *repeat, double *tscal, double *tzero,
           long *tnull, char *tdisp, int  *status);
%clear  int colnum, char *ttype, char *tunit,char *dtype, long *repeat, double *tscal, double *tzero,long *tnull, char *tdisp;


%apply int input {int colnum};
%apply long *input_listarray {long naxes[]};
%apply int input_array_size {int naxis};
int fits_write_tdim( fitsfile *fptr, int colnum, int naxis, long naxes[], int *status);
%clear int colnum, long naxes[], int naxis;

%apply int input {int colnum};
%apply int nelements {int maxdim};
%apply int *output {int *naxis};
%apply long *output_listarray {long naxes[]};
int fits_read_tdim(fitsfile *fptr, int colnum, int maxdim, int *naxis, long naxes[], int *status);
%clear  int colnum, int maxdim, int *naxis, long naxes[];

%apply char *input {char *tdimstr};
%apply int input {int colnum};
%apply int nelements {int maxdim};
%apply int *output {int *naxis};
%apply long *output_listarray {long naxes[]};
int fits_decode_tdim(fitsfile *fptr, char *tdimstr, int colnum, int maxdim,
           int *naxis, long naxes[], int *status);
%clear char *tdimstr, int colnum, int maxdim,int *naxis, long naxes[];


%apply long *output {long *nrows};
int fits_get_rowsize(fitsfile *fptr, long *nrows, int *status);
%clear long *nrows;

%apply long input {long firstrow, long firstchar, long nchars};
%apply char *output {unsigned char *value};
int fits_read_tblbytes(fitsfile *fptr, long firstrow, long firstchar, long nchars,unsigned char *values, int *status);
%clear  long firstrow, long firstchar, long nchars,unsigned char *values;

%apply long input {long firstrow, long firstchar, long nchars};
%apply char *input {unsigned char *value};
int fits_write_tblbytes(fitsfile *fptr, long firstrow, long firstchar, long nchars, unsigned char *values, int *status);
%clear long firstrow, long firstchar, long nchars, unsigned char *values;


%apply int input {int colnum};
%apply char *input {char *ttype, char *tform};
int fficol(fitsfile *fptr, int numcol, char *ttype, char *tform, int *status);
%clear int numcol, char *ttype, char *tform;
