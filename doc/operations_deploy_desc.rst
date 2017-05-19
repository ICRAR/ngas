This task will:

* Check that SSH is working on the target host
* Check that ``sudo`` is installed (``sudo`` is used to run commands as root).
* Install all necessary system packages (using the OS-specific package manager)
  for compiling NGAS and its dependencies
* Compile and install a suitable version of python (2.7) if necessary
* Create the ``NGAS_USER`` if necessary
* Proceed with the normal NGAS compilation and installation as performed by
  ``hl.user_deploy``
* Install an ``/etc/init.d`` script for automatic startup of the server.

The user on the target host used for running the ``sudo`` commands is the SSH
user given to fabric via the command line (``fab -u <user>``).

On top of the normal fabric variables used by ``hl.usr_deploy`` the following
additional variables control this script:

+-----------------------------+--------------------------------------+-------------------+
| Variable                    | Description                          | Default value     |
+=============================+======================================+===================+
| NGAS_USER                   | | The user under which the NGAS      | | ``ngas``        |
|                             | | installation will take place       |                   |
+-----------------------------+--------------------------------------+-------------------+
| NGAS_EXTRA_PACKAGES         | | Comma-separated list of extra      | | Not specified   |
|                             | | system-level packages to install   |                   |
|                             | | on the target system(s)            |                   |
+-----------------------------+--------------------------------------+-------------------+

Currently supported OSs are Ubuntu, Debian, Fedora, CentOS, and MacOSX Darwin,
but more might work or could be added in the future.
