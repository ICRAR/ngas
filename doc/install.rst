############
Installation
############

Installing NGAS is pretty straight-forward.
There are two ways of running the installation:
:ref:`manually installing it <inst.manual>`,
or :ref:`letting fabric do it for you <inst.fabric>`.


.. _inst.manual:

Manual installation
===================

Preparing the installation area
-------------------------------

.. note::
 This step is optional,
 as you might have already a virtual environment
 to install NGAS onto,
 or you may want to have a system-wide installation instead.

First of all, you may wish to create
a `virtual environment <https://virtualenv.readthedocs.org/en/latest/>`_
to install NGAS on it.
You can either do it manually yourself
or go to the NGAS root directory and run::

 ./boot.sh

This command will check you have an appropriate python version,
will get the ``virtualenv`` tool, create a virtual environment,
install a set of basic packages needed later,
and tell you to source the virtual environment.

Installing
----------

To install NGAS go to the root directory and run::

 ./build.sh

Run ``./build.sh -h`` to see the full set of options.
The script will (optionally) build and install the C NGAS client first, and then will build
and install each of the python modules (i.e., ``pcc``, ``ngamsCore``,
``ngamsPClient``, ``ngamsServer`` and ``ngamsPlugIns``). The python modules will
automatically pull and install their dependencies.

.. note::
 If any step of the build fails the script will stop and notify the user.
 It is the user's responsibility to install any missing build dependencies.
 To avoid these issues you can also try
 the :ref:`Fabric installation  <inst.fabric>`

In addition, the ``build.sh`` script recognizes when a virtual environment is loaded,
and will install the C client on it, as well as the python modules.

The C client compilation is based on ``autotools``, meaning that it can also be manually
compiled and installed easily via the usual::

 $> ./bootstrap
 $> ./configure
 $> make all
 $> make install

The Python modules are all setuptool-based packages, meaning that they can also
be manually compiled and installed easily via the usual::

 $> python setup.py install


.. _inst.fabric:

Via Fabric
==========

.. note::
 The installation via Fabric always install NGAS in a virtualenv

`Fabric <http://www.fabfile.org/>`_ is a tool that allows
to perform commands in one or more hosts, local or remote (via SSH).
NGAS comes with a set of fabric modules to ease
the installation of NGAS in complex scenarios,
and to automate most of the system-level preparation tasks.
This enables not only a simple procedure
for installing NGAS in any host or hosts
at a given time,
but also the customization of the hosts as necessary,
plus any other extra step required by other scenarios.

Fabric's command-line allows users to specify the username and hosts where tasks
will take place, and a set of variables to be defined. For example::

 fab -H host.company.com -u ngas_user some_task --set VAR1=a,VAR2

In the example the instructions of the task ``some_task`` will be carried out in
host ``host.company.com`` with the user ``ngas_user``, and the ``VAR1`` variable
will be set to the value ``a``, while variable ``VAR2`` will be marked as set.

For a complete list of tasks run ``fab -l``.
For a more complete manual visit Fabric's `documentation
<http://docs.fabfile.org/en/latest/>`_.

The two main fabric tasks NGAS provides are:

* :ref:`User installation<inst.fabric.user>`:
  This task compiles and installs NGAS under a user-owned directory.
* :ref:`System installation<inst.fabric.system>`:
  The recommended task if you have `sudo` access to the target machine.
  This task installs all the necessary dependencies in the system
  before compiling and installing NGAS.


.. _inst.fabric.user:

Basic per-user installation
---------------------------

To install NGAS in a per-user installation run::

 fab hl.user_deploy

This will first that SSH is working on the target host,
then copy the NGAS sources to the target host,
compile and install NGAS into a virtualenv,
create a default NGAS data directory
with a valid configuration file with which an NGAS server can be started,
and finally modify the user's ``~/.bash_profile`` file
to automatically load the virtualenv when entering a shell.
The per-user installation doesn't take care of installing any dependencies
needed by NGAS, assuming they all are met. For a more complete automatic
procedure that takes care of that see :ref:`inst.fabric.system`.

The following fabric variables (set via the ``--set`` command-line switch)
are available to further customize the process:

.. The auxiliary | are there to allow linebraking in individual cells.
   Cells with one line still have them for nice alignment

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
| NGAS_USER                   | | The user under which the NGAS      | | ``ngas`` in     |
|                             | | installation will take place       | | remote systems, |
|                             |                                      | | current user in |
|                             |                                      | | localhost       |
+-----------------------------+--------------------------------------+-------------------+
| NGAS_REV                    | | The git revision of the sources    | | ``HEAD``        |
|                             | | used to compile and install NGAS   |                   |
|                             | | (only for sources from a git       |                   |
|                             | | repository)                        |                   |
+-----------------------------+--------------------------------------+-------------------+
| NGAS_OVERWRITE_INSTALLATION | | Whether an existing installation   | | ``False``       |
|                             | | directory should be overwritten    |                   |
|                             | | or not                             |                   |
+-----------------------------+--------------------------------------+-------------------+
| NGAS_NO_CLIENT              | | Skip the compilation and           | | ``False``       |
|                             | | installation of the NGAS C client  |                   |
+-----------------------------+--------------------------------------+-------------------+
| NGAS_DEVELOP                | | If specified, install the NGAS     | | Not specified   |
|                             | | Python modules in development mode |                   |
+-----------------------------+--------------------------------------+-------------------+
| NGAS_NO_BASH_PROFILE        | | If specified, skip the edition of  | | Not specified   |
|                             | | the user's ``~/.bash_profile`` for |                   |
|                             | | automatic virtualenv sourcing      |                   |
+-----------------------------+--------------------------------------+-------------------+

For example,
to install the tip of the ``v8`` branch
as user ``foo`` in hosts ``bar1`` and ``bar2``,
and without compiling the C client,
the following command would do::

 fab hl.user_deploy -H bar1,bar2 --set NGAS_USER=foo,NGAS_NO_CLIENT,NGAS_REV=v8

.. _inst.fabric.system:

Total system setup
------------------

.. note::
 ``sudo`` must be installed and configured in the target host
 for this task to work properly.

To perform a system-wide setup and NGAS install run::

 fab hl.operations_deploy

System-wide installation first checks
that SSH is working on the target host
and that ``sudo`` is installed
(``sudo`` is used to run commands as root).
It then installs all necessary system packages
(using the OS-specific package manager)
for compiling NGAS and its dependencies,
creates the ``NGAS_USER`` if necessary
and then proceeds with the rest of the installation
as explained in :ref:`per-user installation <inst.fabric.user>`.

The fabric options from :ref:`per-user installation <inst.fabric.user>`
also apply to the system-wide setup.

Currently supported OSs are Ubuntu, Debian, Fedora, CentOS, and MacOSX Darwin,
but more might work or could be added in the future.

AWS deployment
--------------

.. note::

 The ``boto`` module is required for using this install option.

The fabric modules contain also routines to create an NGAS installation on AWS
machines. This is performed by running::

 fab hl.aws_deploy

This procedure will create and bring up the required AWS instances, and perform
a fabric :ref:`system installation <inst.fabric.system>`.

On top of the normal fabric variables that control the NGAS installation,
these additional variables control the AWS-related aspects of the script:

.. The auxiliary | are there to allow linebraking in individual cells.
   Cells with one line still have them for nice alignment

+-----------------------------+--------------------------------------+-------------------+
| Variable                    | Description                          | Default value     |
+=============================+======================================+===================+
| AWS_PROFILE                 | | The profile to use when connecting | | ``NGAS``        |
|                             | | to AWS                             |                   |
+-----------------------------+--------------------------------------+-------------------+
| AWS_REGION                  | | The AWS region to connect to       | | ``us-east-1``   |
+-----------------------------+--------------------------------------+-------------------+
| AWS_KEY_NAME                | | The private SSH key to be used to  | | ``icrar_ngas``  |
|                             | | create the instances, and later to |                   |
|                             | | connect to them                    |                   |
+-----------------------------+--------------------------------------+-------------------+
| AWS_AMI_NAME                | | The name associated to an AMI      | | ``Amazon``      |
|                             | | (from a predetermined set of AMI   |                   |
|                             | | IDs) which will be used to create  |                   |
|                             | | the instance                       |                   |
+-----------------------------+--------------------------------------+-------------------+
| AWS_INSTANCES               | | The number of instances to create  | | ``1``           |
+-----------------------------+--------------------------------------+-------------------+
| AWS_INSTANCE_TYPE           | | The type of instances to create    | | ``t1.micro``    |
+-----------------------------+--------------------------------------+-------------------+
| AWS_INSTANCE_NAME           | | The name of instances to create    | | ``NGAS_<rev>``  |
+-----------------------------+--------------------------------------+-------------------+
| AWS_SEC_GROUP               | | The name of the security group to  | | ``NGAS``        |
|                             | | attach to the instances (will be   |                   |
|                             | | created if it doesn't exist)       |                   |
+-----------------------------+--------------------------------------+-------------------+
| AWS_ELASTIC_IPS             | | A comma-separated list of public   | | Not specified   |
|                             | | IPs to associate with the new      |                   |
|                             | | instances, if specified.           |                   |
+-----------------------------+--------------------------------------+-------------------+

For example, to create 3 instances of type ``t3.micro`` on region ``us-east-2``
one would run::

 fab hl.aws_deploy --set AWS_REGION=us-east-2,AWS_INSTANCES=3,AWS_INSTANCE_TYPE=t3.micro

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
