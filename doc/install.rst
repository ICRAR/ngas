############
Installation
############

Installing NGAS is pretty straight-forward. There are two ways of running the
installation: manually installing it, or letting fabric do it for you.

Manual installation
===================

To install NGAS go to the ``src`` directory and run::

 ./build.sh

Run ``./build.sh -h`` to see the full set of options.
The script will (optionally) build and install the C NGAS client first, and then will build
and install each of the python modules (i.e., ``pcc``, ``ngamsCore``,
``ngamsPClient``, ``ngamsServer`` and ``ngamsPlugIns``). The python modules will
automatically pull and install their dependencies.

In addition, the ``build.sh`` script recognizes when a `virtual environment
<https://virtualenv.readthedocs.org/en/latest/>`_ is loaded, and will install
the C client there, as well as the python modules.

The C client is an autotools-based program, meaning that it can also be manually
compiled and installed easily via the usual::

 $> ./bootstrap
 $> ./configure
 $> make all
 $> make install

The Python modules are all setuptool-based packages, meaning that they can also
be manually compiled and installed easily via the usual::

 $> python setup.py install

Via Fabric
==========


A set of `fabric <http://www.fabfile.org/>`_ modules have been written to ease
the installation of NGAS in more complex scenarios. Fabric is a tool that allows
to perform commands in one or more hosts, local or remote. Using this we perform
not only the installation of NGAS in any host, but also the customization of the
host as necessary, plus any other extra step required by other scenarios.

Fabric's command-line allows users to specify the username and hosts where tasks
will take place, and a set of variables to be defined. For example::

 fab -H host.company.com -u ngas_user some_task --set VAR1=a,VAR2

In the example the instructions of the task ``some_task`` will be carried out in
host ``host.company.com`` with the user ``ngas_user``, and the ``VAR1`` variable
will be set to the value ``a``, while variable ``VAR2`` will be marked as set.

For a more complete manual visit Fabric's `documentation
<http://docs.fabfile.org/en/1.10/>`_.


Basic per-user installation
---------------------------

To install NGAS in a per-user installation run::

 fab hl.user_deploy

This will compile NGAS and install it under ``~/ngas_rt`` using a `virtual
environment <https://virtualenv.readthedocs.org/en/latest/>`_ for that.
The installation directory will not be overwritten if it exists,
unless the ``NGAS_OVERWRITE_INSTALLATION`` variable is set.
To bypass the compilation and installation of the C client set the
``NGAS_NO_CLIENT`` fabric variable.
Finally, an NGAS data directory will also be created under ``~/NGAS``,
containing a valid configuration file with which an NGAS server can be started.

By default the per-user installation will be performed using the ``ngas`` user
if connecting to a remote system, or using the current username if performing
the installation in the local machine. This behavior can be overridden by
providing a value in the ``NGAS_USER`` fabric variable.

By default the current ``HEAD`` of the git repository will be installed. This
can be overridden by providing a value to the ``NGAS_REV`` fabric variable
indicating the revision that should be installed instead.

The per-user installation doesn't take care of installing any dependencies
needed by NGAS, assuming they all are met. For a more complete automatic
procedure that takes care of that see `Total system setup`_.


Total system setup
------------------

A more complete procedure is available that not only installs NGAS but also takes
care of installing any system-level dependencies needed by NGAS at compilation
time and runtime.

To perform a system-wide setup and NGAS install run::

 fab hl.operations_deploy

System-wide access is performed by running remote commands under ``sudo``, and
therefore proper sudo access must be granted to the user being used to connect
to the remote host.

System-wide installation creates the ``ngas`` user if needed, installs
system-wide packages (using the OS-specific package manager), installs NGAS into
the appropriate user's home directory, and installs a start-up script. The
currently supported OSs are Ubuntu, Debian, Fedora, CentOS, and MacOSX Darwin,
but more might work or could be added.


AWS deployment
--------------

.. note::

 The ``boto`` module is required for using this install option.

The fabric modules contain also routines to create an NGAS installation on AWS
machines. This is performed by running::

 fab hl.aws_deploy

This procedure will create and bring up the required AWS instances, and perform
a fabric system installation

Several fabric variables control the deployment:

* ``AWS_PROFILE``, defaults to ``NGAS``, indicates the profile to use when
  connecting to AWS.
* ``AWS_REGION``, defaults to ``us-east-1``, indicates the region to connect to.
* ``AWS_KEY_NAME``, defaults to ``icrar_ngas``, indicates the private SSH key to
  be used to create the instances, and in the future to connect to them.
* ``instance_type``, defaults to ``t1.micro``, indicates the type of instance to
  create.
* ``AMI_NAME``, defaults to ``Amazon``, indicates the name associated to an AMI
  from a predetermined set of AMI IDs, which will be used to create the
  instance.

Optionally one can also create more than one instance like this::

 fab hl.aws_deploy:n_instances=3

To assist with AWS-related procedures the following other tasks are also
available::

 fab aws.list_instances
 fab aws.terminate_instance:instance_id=<the-instance-id>

Docker Image
------------

.. note::

 The ``docker-py`` module is required for use of this install option.

.. note::

 A local docker daemon must be running and the current user must have access to
 start/stop/build, etc, container and images, this cannot be via sudo!


To create a Docker container containing an NGAS installation simply run::

 fab hl.docker_image

This will generate an image called ``ngas:latest``. When started, the container
by default will be running the NGAS server.

How It is Implemented
^^^^^^^^^^^^^^^^^^^^^

To generate the image the following steps are taken:

1. A stage1 image is built, based on the ``centos:centos7`` image, which includes the
   required installed packages plus also setup for ssh access. The current
   user's ``id_ras.pub`` file is used to put in place a
   ``/root/.ssh/authorized_keys`` file so that ssh access can be performed
   without a password. The IP address of the running docker container is
   obtained and fabric environment updated to use that IP address.
2. The stage1 image is started (becoming the stage1 container) and normal ssh
   based, operations_deploy, is performed via ssh.
3. Once complete the stage1 container is stopped and a commit is done to
   generate a stage2 image. The stage1 container  and stage1 image are both
   removed.
4. A build is done against the stage2 image to generate the final image. The
   build does some basic tidy up plus sets the startup command to run
   ``ngamsServer``, as the ``ngas`` user, on container startup.
