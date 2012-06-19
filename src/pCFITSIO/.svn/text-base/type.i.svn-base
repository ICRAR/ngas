%typemap(python,ignore) fitsfile **fptr {
	fitsfile *tmp;

	$target = &tmp;
}

%typemap(python,argout) fitsfile **fptr {
	char str[512];
	
	SWIG_MakePtr(str, &$source,"_fitsfile_p");
	return PyString_FromString(str);
}

%typemap(python,check) delfitsfile *fptr {
	Py_XDECREF($source);
}
/***********************************************************************/
%typemap(python,ignore)  int *output,  int *status {
  int tmp;
  tmp=0;
  $target=&tmp;
}
%typemap(python,argout)  int *status {
	char str[255];

	if(*$source!=0) {
		ffgerr(*$source,str);
		printf("PFITSIO: %s\n",str);fflush(stdout);
	/* A file IO error triggers a Python exception */
		if ((*$source >= 100) && (*$source < 120)) {
			return ((PyObject *) (PyErr_SetString(PyExc_IOError,str),0));
		}
		if ((*$source >= 120) ) {
			return ((PyObject *) (PyErr_SetString(PyExc_Exception,str),0));
		}
	}
}
/***********************************************************************/
%typemap(python,ignore)  double *output {
	double tmp;
	tmp=0;
	$target=&tmp;
}

%typemap(python,ignore)  float *output {
	float tmp;
	tmp=0;
	$target=&tmp;
}

%typemap(python,ignore)  long *output {
	long tmp;
	tmp=0;
	$target=&tmp;
}
%typemap(python,ignore)  char *output {
	char tmp[FLEN_CARD];
	$target = tmp;
}
/***********************************************************************/
%typemap(python,argout)  int *output {
	PyObject *o;
	int tmp;

	tmp = (int) *$source;

	o = PyInt_FromLong(tmp);
	if ((!$target) || ($target == Py_None)) {
		$target = o;
	} else {
		if (!PyList_Check($target)) {
			PyObject *o2 = $target;
			$target = PyList_New(0);
			PyList_Append($target,o2);
			Py_XDECREF(o2);
		}
		PyList_Append($target,o);
		Py_XDECREF(o);
	}
}

%typemap(python,argout)  long *output {
	PyObject *o;
	long tmp;

	tmp = (long) *$source;

	o = PyInt_FromLong(tmp);
	if ((!$target) || ($target == Py_None)) {
		$target = o;
	} else {
		if (!PyList_Check($target)) {
			PyObject *o2 = $target;
			$target = PyList_New(0);
			PyList_Append($target,o2);
			Py_XDECREF(o2);
		}
		PyList_Append($target,o);
		Py_XDECREF(o);
	}
}

%typemap(python,argout)  double *output, float *output {
	PyObject *o;
	double tmp;

	tmp = (double) *$source;

	o = PyFloat_FromDouble(tmp);
	if ((!$target) || ($target == Py_None)) {
		$target = o;
	} else {
		if (!PyList_Check($target)) {
			PyObject *o2 = $target;
			$target = PyList_New(0);
			PyList_Append($target,o2);
			Py_XDECREF(o2);
		}
		PyList_Append($target,o);
		Py_XDECREF(o);
	}
}

%typemap(python,argout)  char *output {
	PyObject *o;

	o = PyString_FromString($source);

	if ((!$target) || ($target == Py_None)) {
		$target = o;
	} else {
		if (!PyList_Check($target)) {
			PyObject *o2 = $target;
			$target = PyList_New(0);
			PyList_Append($target,o2);
			Py_XDECREF(o2);
		}
		PyList_Append($target,o);
		Py_XDECREF(o);
	}
}


%typemap(python,in) fitsfile2 input {
	if (PyInt_Check($source)) {
		$target = (fitsfile *) NULL;
		/*printf("fitsfile pointer is NULL\n");*/
	}
	else {
		/*printf("fitsfile pointer is NOT NULL\n");*/
		$target = (fitsfile *) $source;
	}



}

/***********************************************************************/
%typemap(python,arginit) void *input {
	$target=NULL;
}

%typemap(python,arginit) double input {
	$target=0.0;
}
%typemap(python,in) double input {
	if (!PyFloat_Check($source)) {
		return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"Float expected"),0));
	}
	$target = PyFloat_AsDouble($source);
}

%typemap(python,arginit) float input {
	$target=0.0;
}
%typemap(python,in) float input {
	if (!PyFloat_Check($source)) {
		return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"Float expected"),0));
	}
	$target = PyFloat_AsDouble($source);
}


%typemap(python,arginit) int input {
	$target=0;
}
%typemap(python,in) int input {
	if (!PyInt_Check($source)) {
		return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"Int expected"),0));
	}
	$target = PyInt_AsLong($source);
}


%typemap(python,arginit) long input {
	$target=0;
}
%typemap(python,in) long input {
	if (!PyInt_Check($source)) {
		return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"Int expected"),0));
	}
	$target = PyInt_AsLong($source);
}

%typemap(python,arginit) char *input {
	$target=NULL;
}
%typemap(python,in) char *input {
	if (!PyString_Check($source)) {
		return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"String expected"),0));
	}
	$target=PyString_AsString($source);
}

%typemap(python,arginit) singlechar *input {
	$target=NULL;
}
%typemap(python,in) singlechar *input {
	char *tmp;
	if (!PyString_Check($source)) {
		return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"String expected"),0));
	}
	tmp = PyString_AsString(_obj5);
	$target=(&tmp)[0];
}

/***********************************************************************/
%typemap(python,arginit) unsigned char *input_listarray {
	$target=NULL;
}
%typemap(python,in) unsigned char *input_listarray {
	long i,size=0;

	if (PyList_Check($source)) {
		size = PyList_Size($source);
	}
	else {
		return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"List expected"),0));
	}

	$target = (unsigned char *) calloc(size,sizeof(unsigned char));
	if ($target == NULL) return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"Could not allocate memory"),0));

	for (i=0;i<size;i++) {
		$target[i]=(unsigned char) PyInt_AsLong(PyList_GetItem($source,i));
	}
	*sizeptr=size;
}
%typemap(python,freearg) unsigned short *input_listarray {
	free($source);
}
/***********************************************************************/
%typemap(python,arginit) char *input_listarray {
	$target=NULL;
}
%typemap(python,in) char *input_listarray {
	long i,size=0;

	if (PyList_Check($source)) {
		size = PyList_Size($source);
	}
	else {
		return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"List expected"),0));
	}

	$target = (char *) calloc(size,sizeof(char));
	if ($target == NULL) return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"Could not allocate memory"),0));

	for (i=0;i<size;i++) {
		$target[i]= (char) PyInt_AsLong(PyList_GetItem($source,i));
	}
	*sizeptr=size;
}
%typemap(python,freearg) unsigned short *input_listarray {
	free($source);
}
/***********************************************************************/
%typemap(python,arginit) unsigned short *input_listarray {
	$target=NULL;
}
%typemap(python,in) unsigned short *input_listarray {
	long i,size=0;

	if (PyList_Check($source)) {
		size = PyList_Size($source);
	}
	else {
		return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"List expected"),0));
	}


	$target = (unsigned short *) calloc(size,sizeof(unsigned short));
	if ($target == NULL) return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"Could not allocate memory"),0));

	for (i=0;i<size;i++) {
		$target[i]=(unsigned short) PyInt_AsLong(PyList_GetItem($source,i));
	}
	*sizeptr=size;
}
%typemap(python,freearg) unsigned short *input_listarray {
	free($source);
}
/***********************************************************************/
%typemap(python,arginit) short *input_listarray {
	$target=NULL;
}
%typemap(python,in) short *input_listarray {
	long i,size=0;

	if (PyList_Check($source)) {
		size = PyList_Size($source);
	}
	else {
		return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"List expected"),0));
	}

	$target = (short *) calloc(size,sizeof(short));
	if ($target == NULL) return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"Could not allocate memory"),0));

	for (i=0;i<size;i++) {
		$target[i]=(short) PyInt_AsLong(PyList_GetItem($source,i));
	}
	*sizeptr=size;
}
%typemap(python,freearg) short *input_listarray {
	free($source);
}
/***********************************************************************/
%typemap(python,arginit) long *input_listarray {
	$target=NULL;
}
%typemap(python,in) long *input_listarray {
	long i,size=0;

	if (PyList_Check($source)) {
		size = PyList_Size($source);
	}
	else {
		return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"List expected"),0));
	}

	$target = (long *) calloc(size,sizeof(long));
	if ($target == NULL) return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"Could not allocate memory"),0));

	for (i=0;i<size;i++) {
		$target[i]= (long) PyInt_AsLong(PyList_GetItem($source,i));
	}
	*sizeptr=size;
}
%typemap(python,freearg) long *input_listarray {
	free($source);
}
/***********************************************************************/
%typemap(python,arginit) unsigned long *input_listarray {
	$target=NULL;
}
%typemap(python,in) unsigned long *input_listarray {
	long i,size=0;

	if (PyList_Check($source)) {
		size = PyList_Size($source);
	}
	else {
		return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"List expected"),0));
	}

	$target = (unsigned long *) calloc(size,sizeof(unsigned long));
	if ($target == NULL) return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"Could not allocate memory"),0));

	for (i=0;i<size;i++) {
		$target[i]= (unsigned long) PyInt_AsLong(PyList_GetItem($source,i));
	}
	*sizeptr=size;
}
%typemap(python,freearg) unsigned long *input_listarray {
	free($source);
}
/***********************************************************************/
%typemap(python,arginit) int *input_listarray {
	$target=NULL;
}
%typemap(python,in) int *input_listarray {
	long i,size=0;

	if (PyList_Check($source)) {
		size = PyList_Size($source);
	}
	else {
		return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"List expected"),0));
	}

	$target = (int *) calloc(size,sizeof(int));
	if ($target == NULL) return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"Could not allocate memory"),0));

	for (i=0;i<size;i++) {
		$target[i]= (int) PyInt_AsLong(PyList_GetItem($source,i));
	}
	*sizeptr=size;
}
%typemap(python,freearg) int *input_listarray {
	free($source);
}
/***********************************************************************/
%typemap(python,arginit) unsigned int *input_listarray {
	$target=NULL;
}
%typemap(python,in) unsigned int *input_listarray {
	long i,size=0;

	if (PyList_Check($source)) {
		size = PyList_Size($source);
	}
	else {
		return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"List expected"),0));
	}

	$target = (unsigned int *) calloc(size,sizeof(unsigned int));
	if ($target == NULL) return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"Could not allocate memory"),0));

	for (i=0;i<size;i++) {
		$target[i]= (unsigned int) PyInt_AsLong(PyList_GetItem($source,i));
	}
	*sizeptr=size;
}
%typemap(python,freearg) unsigned int *input_listarray {
	free($source);
}
/***********************************************************************/
%typemap(python,arginit) float *input_listarray {
	$target=NULL;
}
%typemap(python,in) float *input_listarray {
	long i,size=0;

	if (PyList_Check($source)) {
		size = PyList_Size($source);
	}
	else {
		return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"List expected"),0));
	}

	$target = (float *) calloc(size,sizeof(float));
	if ($target == NULL) return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"Could not allocate memory"),0));

	for (i=0;i<size;i++) {
		$target[i]= (float) PyFloat_AsDouble(PyList_GetItem($source,i));
	}
	*sizeptr=size;
}
%typemap(python,freearg) float *input_listarray {
	free($source);
}
/***********************************************************************/
%typemap(python,arginit) double *input_listarray {
	$target=NULL;
}
%typemap(python,in) double *input_listarray {
	long i,size=0;

	if (PyList_Check($source)) {
		size = PyList_Size($source);
	}
	else {
		return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"List expected"),0));
	}

	$target = (double *) calloc(size,sizeof(double));
	if ($target == NULL) return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"Could not allocate memory"),0));

	for (i=0;i<size;i++) {
		$target[i]= (double) PyFloat_AsDouble(PyList_GetItem($source,i));
	}
	*sizeptr=size;
}
%typemap(python,freearg) double *input_listarray {
	free($source);
}
/***********************************************************************/
%typemap(python,arginit) Py_complex *input_listarray {
	$target=NULL;
}
%typemap(python,in) Py_complex *input_listarray {
	long i,size=0;
	Py_complex *tmp;

	if (PyList_Check($source)) {
		size = PyList_Size($source);
	}
	else {
		return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"List expected"),0));
	}

	$target = (double *) calloc(size,2*sizeof(double));
	if ($target == NULL) return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"Could not allocate memory"),0));

	for (i=0;i<size;i++) {
	  $target[i]= (double) PyComplex_RealAsDouble(PyList_GetItem($source,i));
	  $target[i+1]= (double) PyComplex_ImagAsDouble(PyList_GetItem($source,i));
	}
	*sizeptr=size;
}
%typemap(python,freearg) double *input_listarray {
	free($source);
}


/****************************************************************************/
%typemap(python,arginit) void *input_listarray {
	$target=NULL;
}
%typemap(python,in) void *input_listarray {
	long i,size=0;
	long *ltmp;
	double *dtmp;
	int ok=0;

	if (PyList_Check($source)) {
		size = PyList_Size($source);
	}
	else {
		return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"List expected"),0));
	}



	if(PyInt_Check(PyList_GetItem($source,0))) {
		*voidtypeptr = 31;
		ltmp = (long *) calloc(size,sizeof(long));
		if (ltmp == NULL) return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"Could not allocate memory"),0));
		for (i=0;i<size;i++) {
			ltmp[i] = PyInt_AsLong(PyList_GetItem($source,i));
		}
		$target = (void *) calloc(size,sizeof(long));
		if ($target == NULL) return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"Could not allocate memory"),0));
		$target = memcpy($target,ltmp,size*sizeof(long));
		free(ltmp);
		*sizeptr=size;
		ok = 1;
	}

	if(PyFloat_Check(PyList_GetItem($source,0))) {
		*voidtypeptr = 82;
		dtmp = (double *) calloc(size,sizeof(double));
		if (dtmp == NULL) return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"Could not allocate memory"),0));
		for (i=0;i<size;i++) {
			dtmp[i] = PyFloat_AsDouble(PyList_GetItem($source,i));
		}
		$target = (void *) calloc(size,sizeof(double));
		if ($target == NULL) return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"Could not allocate memory"),0));
		$target = memcpy($target,dtmp,size*sizeof(double));
		free(dtmp);
		*sizeptr=size;
		ok = 1;
	}
	if (ok!=1) return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"Input data type not supported (PyInt and PyFloat only)"),0));
}

%typemap(python,freearg) void *input_listarray {
	free($source);
}
/***********************************************************************/
/***********************************************************************/
/***********************************************************************/
/* support for list of strings.... */


%typemap(python,arginit) string *input_listarray {
	$target=NULL;
}
%typemap(python,in) string *input_listarray {
	long i,size=0;
	char **arr;
	int ok=0;

	if (PyList_Check($source)) {
		size = PyList_Size($source);
	}
	else {
		return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"List expected"),0));
	}


	/* Allocate an array of string pointers */	
	arr = (char **) malloc(size*sizeof(char *));

	for(i=0;i<size;i++) {
		arr[i] = PyString_AsString(PyList_GetItem($source,i));
		//fprintf(stderr,"%s\n",arr[i]);
	}

	$target = arr;
	*sizeptr=size;

}

%typemap(python,ignore) string *output_listarray {
/* Ignoring this parameter */
}
%typemap(python,check) string *output_listarray {
/* Allocating enough memory for this array */
	int tmp,i;
	tmp = 0;
	tmp = *nelementsptr;
	$target = (char **) malloc(tmp*sizeof(char *));
	if ($target == NULL) return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"Could not allocate memory"),0));
	for (i=0;i<*nelementsptr;i++) {
		$target[i]=malloc(1000*sizeof(char)); /* Alloction some room or some large strings */
		if ($target[i] == NULL) return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"Could not allocate memory"),0));
	}
}

%typemap(python,argout) string *output_listarray {
	PyObject *o;
	int i;
	char **tmp;

	fprintf(stderr,"here\n");
	o = PyList_New(*nelementsptr);
	tmp = (char **) $source;
	
	for (i=0;i<*nelementsptr;i++) {
		fprintf(stderr,"%s\n",tmp[i]);
		PyList_SetItem(o,i,PyString_FromString(tmp[i]));
	}
	if ((!$target) || ($target == Py_None)) {
		$target = o;
	} else {
		if (!PyList_Check($target)) {
			PyObject *o2 = $target;
			$target = PyList_New(0);
			PyList_Append($target,o2);
			Py_XDECREF(o2);
		}
		PyList_Append($target,o);
		Py_XDECREF(o);
	}
	for (i=0;i<*nelementsptr;i++) free($source[i]);
	free($source);
}


/***********************************************************************/
/***********************************************************************/
/***********************************************************************/
/***********************************************************************/





/***********************************************************************/
%typemap(python,arginit) void *input_numarray {
	$target=NULL;
}
%typemap(python,in) void *input_numarray {
	PyArrayObject *ap;
	//fprintf(stderr," 1 type of array: %d",NA_maxType($source));

	if (NA_maxType($source) == 3) {
		/*printf("double\n");fflush(stdout);*/
		ap = (PyArrayObject *)PyArray_CopyFromObject($source, PyArray_DOUBLE, 1, 0);
		$target = (void*)ap->data;
		*voidtypeptr = 82;
	}

	if (NA_maxType($source) == 1) {
		/*printf("long\n");fflush(stdout);*/

		ap = (PyArrayObject *)PyArray_CopyFromObject($source, PyArray_LONG, 1, 0);
		$target = (void*)ap->data;
		*voidtypeptr = 31;
	}
	//fprintf(stderr,"here!");

}
%typemap(python,freearg) void *input_numarray {
	//free($source);
}


%typemap(python,arginit) double *input_array {
	$target=NULL;
}
%typemap(python,in) double *input_array {
	PyArrayObject *ap;

	//fprintf(stderr," 2 type of array: %d",NA_maxType($source));

	if (NA_maxType($source) == 3) {
		printf("double\n");fflush(stdout);	
		ap = (PyArrayObject *)PyArray_CopyFromObject($source, PyArray_DOUBLE, 1, 0);
		$target = (double*)ap->data;
	}
}

%typemap(python,arginit) long *input_array {
	$target=NULL;
}
%typemap(python,in) long *input_array {
	PyArrayObject *ap;
	//fprintf(stderr,"3 type of array: %d",NA_maxType($source));

	if (NA_maxType($source) == 1) {
		printf("long\n");fflush(stdout);	
		ap = (PyArrayObject *)PyArray_CopyFromObject($source, PyArray_LONG, 1, 0);
		$target = (long*)ap->data;
	}
}

/***********************************************************************/
%typemap(python,arginit) int out_array_size (int *out_size){
	out_size = &$target;
}
%typemap(python,ignore) int *out_array_nfound (int *out_nfoundptr){
	int tmp;
	tmp=0;
	$target=&tmp;
	out_nfoundptr=&tmp;
}
%typemap(python,ignore) long *out_array {
}
%typemap(python,check) long *out_array {
	long *tmp;
	tmp = calloc(*out_size,sizeof(long));
	if (tmp == NULL) return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"Could not allocate memory"),0));
	$target=tmp;
}
%typemap(python,argout) long *out_array {
	PyObject *o;
	int i;
	long *ltmp;
	ltmp = (long *)$source;
	o = PyList_New(*out_nfoundptr);

	for (i=0;i<*out_nfoundptr;i++) {
		PyList_SetItem(o,i,PyInt_FromLong($source[i]));
	}
	if ((!$target) || ($target == Py_None)) {
		$target = o;
	} else {
		if (!PyList_Check($target)) {
			PyObject *o2 = $target;
			$target = PyList_New(0);
			PyList_Append($target,o2);
			Py_XDECREF(o2);
		}
		PyList_Append($target,o);
		Py_XDECREF(o);
	}
	free($source);
}
%typemap(python,ignore) double *out_array {
}
%typemap(python,check) double *out_array {
	double *tmp;
	tmp = calloc(*out_size,sizeof(double));
	if (tmp == NULL) return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"Could not allocate memory"),0));
	$target=tmp;
}
%typemap(python,argout) double *out_array {
	PyObject *o;
	int i;
	double *dtmp;
	dtmp = (double *)$source;
	o = PyList_New(*out_nfoundptr);

	for (i=0;i<*out_nfoundptr;i++) {
		PyList_SetItem(o,i,PyFloat_FromDouble($source[i]));
	}
	if ((!$target) || ($target == Py_None)) {
		$target = o;
	} else {
		if (!PyList_Check($target)) {
			PyObject *o2 = $target;
			$target = PyList_New(0);
			PyList_Append($target,o2);
			Py_XDECREF(o2);
		}
		PyList_Append($target,o);
		Py_XDECREF(o);
	}
	free($source);
}

%typemap(python,ignore) char *out_array[] {
}
%typemap(python,check) char *out_array[] {
	char **tmp;
	int i;
	tmp = (char **)calloc(*out_size,sizeof(char *));
	if (tmp == NULL) return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"Could not allocate memory"),0));
	for (i=0;i<*out_size;i++) {
		tmp[i]=(char *) calloc(255,sizeof(char));
		if (tmp[i] == NULL) return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"Could not allocate memory"),0));
	}
	$target=(char **)tmp;
}
%typemap(python,argout) char *out_array[] {
	PyObject *o;
	int i;
	o = PyList_New(*out_nfoundptr);
	for (i=0;i<*out_nfoundptr;i++) {
		PyList_SetItem(o,i,PyString_FromString($source[i]));
	}
	if ((!$target) || ($target == Py_None)) {
		$target = o;
	} else {
		if (!PyList_Check($target)) {
			PyObject *o2 = $target;
			$target = PyList_New(0);
			PyList_Append($target,o2);
			Py_XDECREF(o2);
		}
		PyList_Append($target,o);
		Py_XDECREF(o);
	}
	for (i=0;i<*out_size;i++) free($source[i]);
	free($source);
}

/***********************************************************************/
%typemap(python,ignore) void *output_listarray {
  /*void *tmp;
    $target=tmp;*/
  void * $target;
	$target=NULL;
}
%typemap(python,check) void *output_listarray {
	int tmp;
	tmp = 0;
	tmp = *nelementsptr;
	switch (*voidtypeptr) {
	case 31:
		$target = (void *)calloc(tmp,sizeof(long));
		if ($target == NULL) return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"Could not allocate memory"),0));
		break;
	case 82:
		$target = (void *)calloc(tmp,sizeof(double));
		if ($target == NULL) return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"Could not allocate memory"),0));
		break;
	default:
		$target = (void *)calloc(tmp,sizeof(double));
		if ($target == NULL) return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"Could not allocate memory"),0));
		break;
	}
}
%typemap(python,argout) void *output_listarray {
	PyObject *o;
	int i;
	long *ltmp;
	double *dtmp;

	o = PyList_New(*nelementsptr);
	switch (*voidtypeptr) {
	case 31:
		ltmp = (long *)$source;
		for (i=0;i<*nelementsptr;i++) {
			 PyList_SetItem(o,i,PyInt_FromLong(ltmp[i]));
		}
		break;
	case 82:
		dtmp = (double *)$source;	
		for (i=0;i<*nelementsptr;i++) {
			if (*voidtypeptr == 82) PyList_SetItem(o,i,PyFloat_FromDouble(dtmp[i]));
		}
		break;
	default:
		dtmp = (double *)$source;	
		for (i=0;i<*nelementsptr;i++) {
			if (*voidtypeptr == 82) PyList_SetItem(o,i,PyFloat_FromDouble(dtmp[i]));
		}
		break;
	}
	if ((!$target) || ($target == Py_None)) {
		$target = o;
	} else {
		if (!PyList_Check($target)) {
			PyObject *o2 = $target;
			$target = PyList_New(0);
			PyList_Append($target,o2);
			Py_XDECREF(o2);
		}
		PyList_Append($target,o);
		Py_XDECREF(o);
	}
	free($source);
}
/***********************************************************************/
%typemap(python,ignore) unsigned char *output_listarray {
/* Ignoring this parameter */
}

%typemap(python,check) unsigned char *output_listarray {
/* Allocating enough memory for this array */
	int tmp;
	tmp = 0;
	tmp = *nelementsptr;
	$target = (unsigned char *) calloc(tmp,sizeof(unsigned char));
	if ($target == NULL) return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"Could not allocate memory"),0));
}

%typemap(python,argout) unsigned char *output_listarray {
	PyObject *o;
	int i;
	long *tmp;

	o = PyList_New(*nelementsptr);
	tmp = (long *)$source;
	
	for (i=0;i<*nelementsptr;i++) {
		PyList_SetItem(o,i,PyInt_FromLong(tmp[i]));
	}
	if ((!$target) || ($target == Py_None)) {
		$target = o;
	} else {
		if (!PyList_Check($target)) {
			PyObject *o2 = $target;
			$target = PyList_New(0);
			PyList_Append($target,o2);
			Py_XDECREF(o2);
		}
		PyList_Append($target,o);
		Py_XDECREF(o);
	}
	free($source);
}
/***********************************************************************/
%typemap(python,ignore) char *output_listarray {
/* Ignoring this parameter */
}

%typemap(python,check) char *output_listarray {
/* Allocating enough memory for this array */
	int tmp;
	tmp = 0;
	tmp = *nelementsptr;
	$target = (char *) calloc(tmp,sizeof(char));
	if ($target == NULL) return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"Could not allocate memory"),0));
}

%typemap(python,argout) char *output_listarray {
	PyObject *o;
	int i;
	long *tmp;

	o = PyList_New(*nelementsptr);
	tmp = (long *)$source;
	
	for (i=0;i<*nelementsptr;i++) {
		PyList_SetItem(o,i,PyInt_FromLong(tmp[i]));
	}
	if ((!$target) || ($target == Py_None)) {
		$target = o;
	} else {
		if (!PyList_Check($target)) {
			PyObject *o2 = $target;
			$target = PyList_New(0);
			PyList_Append($target,o2);
			Py_XDECREF(o2);
		}
		PyList_Append($target,o);
		Py_XDECREF(o);
	}
	free($source);
}
/***********************************************************************/
%typemap(python,ignore) unsigned short *output_listarray {
/* Ignoring this parameter */
}

%typemap(python,check) unsigned short *output_listarray {
/* Allocating enough memory for this array */
	int tmp;
	tmp = 0;
	tmp = *nelementsptr;
	$target = (unsigned short *)calloc(tmp,sizeof(unsigned short));
	if ($target == NULL) return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"Could not allocate memory"),0));
}

%typemap(python,argout) unsigned short *output_listarray {
	PyObject *o;
	int i;
	long *tmp;

	o = PyList_New(*nelementsptr);
	tmp = (long *)$source;	
	for (i=0;i<*nelementsptr;i++) {
		PyList_SetItem(o,i,PyInt_FromLong(tmp[i]));
	}
	if ((!$target) || ($target == Py_None)) {
		$target = o;
	} else {
		if (!PyList_Check($target)) {
			PyObject *o2 = $target;
			$target = PyList_New(0);
			PyList_Append($target,o2);
			Py_XDECREF(o2);
		}
		PyList_Append($target,o);
		Py_XDECREF(o);
	}
	free($source);
}
/***********************************************************************/
%typemap(python,ignore) short *output_listarray {
/* Ignoring this parameter */
}

%typemap(python,check) short *output_listarray {
/* Allocating enough memory for this array */
	int tmp;
	tmp = 0;
	tmp = *nelementsptr;
	$target = (short *)calloc(tmp,sizeof(short));
	if ($target == NULL) return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"Could not allocate memory"),0));
}

%typemap(python,argout) short *output_listarray {
	PyObject *o;
	int i;
	long *tmp;

	o = PyList_New(*nelementsptr);
	tmp = (long *)$source;	
	for (i=0;i<*nelementsptr;i++) {
		PyList_SetItem(o,i,PyInt_FromLong(tmp[i]));
	}
	if ((!$target) || ($target == Py_None)) {
		$target = o;
	} else {
		if (!PyList_Check($target)) {
			PyObject *o2 = $target;
			$target = PyList_New(0);
			PyList_Append($target,o2);
			Py_XDECREF(o2);
		}
		PyList_Append($target,o);
		Py_XDECREF(o);
	}
	free($source);
}
/***********************************************************************/
%typemap(python,ignore) long *output_listarray {
/* Ignoring this parameter */
}

%typemap(python,check) long *output_listarray {
/* Allocating enough memory for this array */
	int tmp;
	tmp = 0;
	tmp = *nelementsptr;
	$target = (long *)calloc(tmp,sizeof(long));
	if ($target == NULL) return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"Could not allocate memory"),0));
}

%typemap(python,argout) long *output_listarray {
	PyObject *o;
	int i;
	long *tmp;

	o = PyList_New(*nelementsptr);
	tmp = (long *)$source;	
	for (i=0;i<*nelementsptr;i++) {
		PyList_SetItem(o,i,PyInt_FromLong((long) tmp[i]));
	}
	if ((!$target) || ($target == Py_None)) {
		$target = o;
	} else {
		if (!PyList_Check($target)) {
			PyObject *o2 = $target;
			$target = PyList_New(0);
			PyList_Append($target,o2);
			Py_XDECREF(o2);
		}
		PyList_Append($target,o);
		Py_XDECREF(o);
	}
	free($source);
}
/***********************************************************************/
%typemap(python,ignore) unsigned long *output_listarray {
/* Ignoring this parameter */
}

%typemap(python,check) unsigned long *output_listarray {
/* Allocating enough memory for this array */
	int tmp;
	tmp = 0;
	tmp = *nelementsptr;
	$target = (unsigned long *)calloc(tmp,sizeof(unsigned long));
	if ($target == NULL) return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"Could not allocate memory"),0));
}

%typemap(python,argout) unsigned long *output_listarray {
	PyObject *o;
	int i;
	long *tmp;

	o = PyList_New(*nelementsptr);
	tmp = (long *)$source;	
	for (i=0;i<*nelementsptr;i++) {
		PyList_SetItem(o,i,PyInt_FromLong(tmp[i]));
	}
	if ((!$target) || ($target == Py_None)) {
		$target = o;
	} else {
		if (!PyList_Check($target)) {
			PyObject *o2 = $target;
			$target = PyList_New(0);
			PyList_Append($target,o2);
			Py_XDECREF(o2);
		}
		PyList_Append($target,o);
		Py_XDECREF(o);
	}
	free($source);
}
/***********************************************************************/
%typemap(python,ignore) int *output_listarray {
/* Ignoring this parameter */
}

%typemap(python,check) int *output_listarray {
/* Allocating enough memory for this array */
	int tmp;
	tmp = 0;
	tmp = *nelementsptr;
	$target = (int *)calloc(tmp,sizeof(int));
	if ($target == NULL) return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"Could not allocate memory"),0));
}

%typemap(python,argout) int *output_listarray {
	PyObject *o;
	int i;
	long *tmp;

	o = PyList_New(*nelementsptr);
	tmp = (long *)$source;	
	for (i=0;i<*nelementsptr;i++) {
		PyList_SetItem(o,i,PyInt_FromLong(tmp[i]));
	}
	if ((!$target) || ($target == Py_None)) {
		$target = o;
	} else {
		if (!PyList_Check($target)) {
			PyObject *o2 = $target;
			$target = PyList_New(0);
			PyList_Append($target,o2);
			Py_XDECREF(o2);
		}
		PyList_Append($target,o);
		Py_XDECREF(o);
	}
	free($source);
}
/***********************************************************************/
%typemap(python,ignore) unsigned int *output_listarray {
/* Ignoring this parameter */
}

%typemap(python,check) unsigned int *output_listarray {
/* Allocating enough memory for this array */
	int tmp;
	tmp = 0;
	tmp = *nelementsptr;
	$target = (unsigned int *)calloc(tmp,sizeof(unsigned int));
	if ($target == NULL) return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"Could not allocate memory"),0));
}

%typemap(python,argout) unsigned int *output_listarray {
	PyObject *o;
	int i;
	long *tmp;

	o = PyList_New(*nelementsptr);
	tmp = (long *)$source;	
	for (i=0;i<*nelementsptr;i++) {
		PyList_SetItem(o,i,PyInt_FromLong(tmp[i]));
	}
	if ((!$target) || ($target == Py_None)) {
		$target = o;
	} else {
		if (!PyList_Check($target)) {
			PyObject *o2 = $target;
			$target = PyList_New(0);
			PyList_Append($target,o2);
			Py_XDECREF(o2);
		}
		PyList_Append($target,o);
		Py_XDECREF(o);
	}
	free($source);
}
/***********************************************************************/
%typemap(python,ignore) float *output_listarray {
/* Ignoring this parameter */
}

%typemap(python,check) float *output_listarray {
/* Allocating enough memory for this array */
	int tmp;
	tmp = 0;
	tmp = *nelementsptr;
	$target = (float *)calloc(tmp,sizeof(float));
	if ($target == NULL) return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"Could not allocate memory"),0));
}

%typemap(python,argout) float *output_listarray {
	PyObject *o;
	int i;
	float *tmp;

	o = PyList_New(*nelementsptr);
	tmp = $source;	
	for (i=0;i<*nelementsptr;i++) {
		PyList_SetItem(o,i,PyFloat_FromDouble((double) tmp[i]));
	}
	if ((!$target) || ($target == Py_None)) {
		$target = o;
	} else {
		if (!PyList_Check($target)) {
			PyObject *o2 = $target;
			$target = PyList_New(0);
			PyList_Append($target,o2);
			Py_XDECREF(o2);
		}
		PyList_Append($target,o);
		Py_XDECREF(o);
	}
	free($source);
}
/***********************************************************************/
%typemap(python,argout) double *output_listarray {
	PyObject *o;
	int i;
	double *tmp;

	o = PyList_New(*nelementsptr);
	tmp = (double *)$source;	
	for (i=0;i<*nelementsptr;i++) {
		PyList_SetItem(o,i,PyFloat_FromDouble(tmp[i]));
	}
	if ((!$target) || ($target == Py_None)) {
		$target = o;
	} else {
		if (!PyList_Check($target)) {
			PyObject *o2 = $target;
			$target = PyList_New(0);
			PyList_Append($target,o2);
			Py_XDECREF(o2);
		}
		PyList_Append($target,o);
		Py_XDECREF(o);
	}
	free($source);
}
/***********************************************************************/
%typemap(python,argout) complex *output_listarray {
	PyObject *o;
	int i;
	float *tmp;

	o = PyList_New(*nelementsptr);
	tmp = (float *)$source;	
	for (i=0;i<*nelementsptr;i+=2) {
		PyList_SetItem(o,i,PyComplex_FromDoubles((double) tmp[i],(double)tmp[i+1]));
	}
	if ((!$target) || ($target == Py_None)) {
		$target = o;
	} else {
		if (!PyList_Check($target)) {
			PyObject *o2 = $target;
			$target = PyList_New(0);
			PyList_Append($target,o2);
			Py_XDECREF(o2);
		}
		PyList_Append($target,o);
		Py_XDECREF(o);
	}
	free($source);
}


%typemap(python,ignore) short input_array_size (short *sizeptr){
	sizeptr = &$target;
}
%typemap(python,ignore) unsigned short input_array_size (unsigned short *sizeptr){
	sizeptr = &$target;
}
%typemap(python,ignore) long input_array_size (long *sizeptr){
	sizeptr = &$target;
}
%typemap(python,ignore) unsigned long input_array_size (unsigned long *sizeptr){
	sizeptr = &$target;
}
%typemap(python,ignore) int input_array_size (int *sizeptr){
	sizeptr = &$target;
}
%typemap(python,ignore) unsigned int input_array_size (unsigned int *sizeptr){
	sizeptr = &$target;
}
%typemap(python,ignore) float input_array_size (float *sizeptr){
	sizeptr = &$target;
}
%typemap(python,ignore) double input_array_size (double *sizeptr){
	sizeptr = &$target;
}



%typemap(python,ignore) int voidtype (int *voidtypeptr){
	voidtypeptr = &$target;
}

%typemap(python,arginit) long nelements (long *nelementsptr){
	nelementsptr = &$target;
}

%typemap(python,arginit) int nelements (long *nelementsptr){
        nelementsptr = (long *)&$target;
}

%typemap(python,in) void *input {
	long *ltmp;
	double *dtmp;
	
	if(PyInt_Check($source)) {
		ltmp = calloc(1,sizeof(long));
		if (ltmp == NULL) return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"Could not allocate memory"),0));
		*voidtypeptr = 31;
		*ltmp=PyInt_AsLong($source);
		$target = (void *)ltmp;
		/*free(ltmp);*/
	}
	if(PyFloat_Check($source)) {
		dtmp = calloc(1,sizeof(double));
		if (dtmp == NULL) return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"Could not allocate memory"),0));
		*voidtypeptr = 82;
		*dtmp=PyFloat_AsDouble($source);
		$target = (void *)dtmp;
		/*free(dtmp);*/
	}
	if(PyString_Check($source)) {
		*voidtypeptr = 16;
		$target=(void *)PyString_AsString($source);
	}

	if (!PyInt_Check($source) && !PyFloat_Check($source) && !PyString_Check($source)) {
		return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"Int, Float, or String expected"),0));
	}

}

%typemap(python,ignore) void *output_numarray {
	void *tmp;
	$target=tmp;
}
%typemap(python,check) void *output_numarray {
	int tmp;	
	tmp = *nelementsptr;
	switch (*voidtypeptr) {
	case 31:
		$target = (void *)calloc(tmp,sizeof(long));
		if ($target == NULL) return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"Could not allocate memory"),0));
		break;
	case 82:
		$target = (void *)calloc(tmp,sizeof(double));
		if ($target == NULL) return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"Could not allocate memory"),0));
		break;
	default:
		$target = (void *)calloc(tmp,sizeof(double));
		if ($target == NULL) return ((PyObject *) (PyErr_SetString(PyExc_TypeError,"Could not allocate memory"),0));
		break;
	}
}
%typemap(python,argout) void *output_numarray {
	int i,naxis=1;
	int naxes[2];
	double *dtmp,*dptr;
	long *ltmp,*lptr;
	PyArrayObject *op=NULL;

	naxes[0]=*nelementsptr;
	naxes[1]=1;

	switch (*voidtypeptr) {
	case 31:
		op = (PyArrayObject *) PyArray_FromDims(naxis, (int *)naxes, PyArray_INT);
		if (op == NULL) return Py_BuildValue("s", "");
		ltmp = (long *)op->data;
		lptr = (long *)$source;
		for (i=0;i<*nelementsptr;i++) {
			*ltmp = (long)lptr[i];
			ltmp++;	
		}
		break;
	case 82:
		op = (PyArrayObject *) PyArray_FromDims(naxis, (int *)naxes, PyArray_DOUBLE);
		if (op == NULL) return Py_BuildValue("s", "");
		dtmp = (double *)op->data;
		dptr = (double *)$source;
		for (i=0;i<*nelementsptr;i++) {
			*dtmp = (double)dptr[i];
			dtmp++;	
		}
		break;
	}
	$target=(PyObject *)op;
	free($source);
}
