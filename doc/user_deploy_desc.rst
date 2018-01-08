This task will:

* Check that SSH is working on the target host
* Copy the NGAS sources to the target host
* Compile and install NGAS into a virtualenv on the target host
* Create a minimal NGAS data directory with a valid configuration file,
  and a valid SQLite database, with which an NGAS server can be started
* Finally, modify the corresponding ``~/.bash_profile`` file to automatically
  load the NGAS virtualenv when the user enters a ``bash`` shell.

The user on the target host used for running these tasks is the SSH user given
to fabric via the command line (``fab -u <user>``).

This task doesn't take care of installing any dependencies needed by NGAS,
assuming they all are met. For a more complete automatic procedure that takes
care of that see the ``hl.operations_deploy`` task.

The following fabric variables (set via the ``--set`` command-line switch)
are available to further customize the process:

+-----------------------------+--------------------------------------+-------------------+
| Variable                    | Description                          | Default value     |
+=============================+======================================+===================+
| NGAS_SRC_DIR                | | The directory where the NGAS       | | ``~/ngas_src``  |
|                             | | sources will be extracted on the   |                   |
|                             | | target host                        |                   |
+-----------------------------+--------------------------------------+-------------------+
| NGAS_INSTALL_DIR            | | The directory where the virtualenv | | ``~/ngas_rt``   |
|                             | | will be created and NGAS           |                   |
|                             | | installed                          |                   |
+-----------------------------+--------------------------------------+-------------------+
| NGAS_ROOT_DIR               | | The NGAS data directory created by | | ``~/NGAS``      |
|                             | | default by the installation        |                   |
|                             | | procedure                          |                   |
+-----------------------------+--------------------------------------+-------------------+
| NGAS_REV                    | | The git revision of the sources    | | ``HEAD``        |
|                             | | used to compile and install NGAS   |                   |
|                             | | (only for sources from a git       |                   |
|                             | | repository)                        |                   |
+-----------------------------+--------------------------------------+-------------------+
| NGAS_OVERWRITE_INSTALLATION | | If specified, an existing          | | Not specified   |
|                             | | installation directory will be     |                   |
|                             | | overwritten                        |                   |
+-----------------------------+--------------------------------------+-------------------+
| NGAS_USE_CUSTOM_PIP_CERT    | | If specified, configure pip to use | | Not specified   |
|                             | | curl.haxx.se/ca/cacert.pem as the  |                   |
|                             | | root TLS certificate. In some old  |                   |
|                             | | platforms this is needed so pip    |                   |
|                             | | trusts PyPI downloads              |                   |
+-----------------------------+--------------------------------------+-------------------+
| NGAS_EXTRA_PYTHON_PACKAGES  | | Comma-separated list of extra      | | Not specified   |
|                             | | python packages to install         |                   |
+-----------------------------+--------------------------------------+-------------------+
| NGAS_NO_CLIENT              | | If specified, skip the compilation | | Not specified   |
|                             | | and installation of the NGAS C     |                   |
|                             | | client                             |                   |
+-----------------------------+--------------------------------------+-------------------+
| NGAS_NO_CRC32C              | | If specified, NGAS will not depend | | Not specified   |
|                             | | (and will avoid installing) the    |                   |
|                             | | ``crc32c`` python package. This is |                   |
|                             | | useful if you know this feature is |                   |
|                             | | not planned to be used at runtime  |                   |
+-----------------------------+--------------------------------------+-------------------+
| NGAS_DEVELOP                | | If specified, install the NGAS     | | Not specified   |
|                             | | Python modules in development mode |                   |
+-----------------------------+--------------------------------------+-------------------+
| NGAS_NO_DOC_DEPENDENCIES    | | If specified, skip the             | | Not specified   |
|                             | | installation of python packages    |                   |
|                             | | needed to build the NGAS           |                   |
|                             | | documentation                      |                   |
+-----------------------------+--------------------------------------+-------------------+
| NGAS_NO_BASH_PROFILE        | | If specified, skip the edition of  | | Not specified   |
|                             | | the user's ``~/.bash_profile`` for |                   |
|                             | | automatic virtualenv sourcing      |                   |
+-----------------------------+--------------------------------------+-------------------+
| NGAS_SERVER_TYPE            | | The server type configured after   | | ``archive``     |
|                             | | installing NGAS (``archive``,      |                   |
|                             | | ``cache``)                         |                   |
+-----------------------------+--------------------------------------+-------------------+
