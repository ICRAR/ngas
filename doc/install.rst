############
Installation
############

.. contents:: Contents
   :local:

Installing from source
======================

First, get the latest NGAS sources::

 git clone https://github.com/ICRAR/ngas

Installing NGAS from source is pretty straight-forward.
There are two ways to perform an installation:

* :ref:`Manually <inst.manual>`.
* :ref:`Using Fabric <inst.fabric>`.
  This is the recommended way, if possible, as it automates most of the
  installation steps while still being highly customisable.

.. _inst.manual:

Manual installation
-------------------

.. note::
 Like any other python package,
 NGAS can be installed in a virtual environment
 or as a system-wide package.

To manually install NGAS go to the root directory and simply run::

 ./build.sh

Run ``./build.sh -h`` to see the full set of options.
Among the options users can choose
whether to skip the build of the C-written clients,
to install the python packages in development mode,
and to build NGAS without CRC32c support
(see :ref:`server.crc` for details).

The script will (optionally) build and install the C NGAS client first, and then will build
and install each of the python modules (i.e., ``ngamsCore``,
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
----------

.. note::
 The installation via Fabric always installs NGAS in a virtualenv

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

 fab -H host.company.com -u user some_task --set VAR1=a,VAR2

In the example the instructions of the task ``some_task`` will be carried out in
host ``host.company.com`` with the user ``user``, and the ``VAR1`` variable
will be set to the value ``a``, while variable ``VAR2`` will be marked as set.

For a complete list of tasks run ``fab -l``.
For a detailed description of a task run ``fab -d <task>``.
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
^^^^^^^^^^^^^^^^^^^^^^^^^^^

To compile and install NGAS in a user-owned directory run::

 fab hl.user_deploy

.. include:: user_deploy_desc.rst

For example,
to install the tip of the ``v8`` branch
as user ``foo`` in hosts ``bar1`` and ``bar2``,
and without compiling the C client,
the following command would do::

 fab hl.user_deploy -u foo -H bar1,bar2 --set NGAS_NO_CLIENT,NGAS_REV=v8

.. _inst.fabric.system:

Total system setup
^^^^^^^^^^^^^^^^^^

.. note::
 ``sudo`` must be installed and configured in the target host
 for this task to work properly.
 Also, the user used with fab (``fab -u <user>``) needs to be properly configured
 on the target host to use ``sudo`` commands.

To perform a system-wide setup and NGAS install run::

 fab hl.operations_deploy

.. include:: operations_deploy_desc.rst


.. _inst.other_fabric:

Other Fabric tasks
------------------

On top of the :ref:`two main Fabric tasks<inst.fabric>` to install NGAS,
our fabric modules define a number of other
optional, high-level tasks that can be useful
in other scenarios.

.. _inst.other_fabric.aws:

AWS deployment
^^^^^^^^^^^^^^

.. note::

 The ``boto`` module is required for using this install option.

The fabric modules contain routines to create an NGAS installation on AWS
machines. This is performed by running::

 fab hl.aws_deploy

.. include:: aws_deploy_desc.rst

For example, to create 3 instances of type ``t3.micro`` on region ``us-east-2``
one would run::

 fab hl.aws_deploy --set AWS_REGION=us-east-2,AWS_INSTANCES=3,AWS_INSTANCE_TYPE=t3.micro

To assist with AWS-related procedures the following other tasks are also
available::

 fab aws.list_instances
 fab aws.terminate_instance:instance_id=<the-instance-id>

.. _inst.other_fabric.docker:

Docker Image
^^^^^^^^^^^^

.. note::
 These instructions are to *build* a docker image with NGAS.
 If you simply want to *use* the pre-built images
 see :ref:`inst.docker`.

.. note::

 The ``docker`` python package is required to use of this install option.
 Also, a local docker daemon must be running
 and the current user must have access to perform docker operations.

To build a Docker container containing an NGAS installation simply run::

 fab hl.docker_image

This will generate an image called ``icrar/ngas:latest`` based on CentOS 7.
When started, the container by default will run the NGAS server.
The NGAS server will look for a configuration file
under ``/home/ngas/NGAS/cfg/ngamsServer.conf``,
which by default needs to be provided via volume mapping.

.. include:: docker_image_desc.rst


.. _inst.docker:

Docker container
================

Alternatively, if you don't need to build NGAS from sources
you can use the pre-built docker image we distribute for NGAS.
To do so you can do::

 docker pull icrar/ngas:latest

Then follow the instructions
in `the DockerHub page <https://hub.docker.com/r/icrar/ngas>`_
to get yourself started.
