Setting up an NGAS instance
===========================

These steps describe how to set up an NGAS server instance.

When using on of the :ref:`fabric-based installation <inst.fabric>` procedures
an NGAS root directory is automatically created and prepared
under ``~/NGAS`` of the user hosting the NGAS installation
(or somewhere different if indicated via the ``NGAS_ROOT`` fabric variable)

Create an NGAS root directory
-----------------------------

NGAS's *root* is the top-level directory
that will be used to store all its internal files,
including the data being stored.

The NGAS root directory can be placed anywhere in the filesystem,
and can be totally empty initially.
The only requirement is that it is writable by the user
running the NGAS server.

To help users create their an NGAS root directory,
NGAS comes with a ``prepare_ngas_root.sh`` script.
The script needs at least
the name of the target directory
that will be used as NGAS root.
More options are available,
you can use ``prepare_ngas_root.sh -h`` to learn more.

The ``prepare_ngas_root.sh`` script
will also create simple, but usable, *volumes*
under the NGAS *root* directory.
These are sufficient for testing purposes,
but you may want to setup proper volumes
(see next section).

Setup volumes
-------------

Inside the NGAS *root* directory
*volumes* should exist
where the data will be stored
(see :ref:`server.storage` for a full explanation).
Volumes need only be directories,
so you can either create directories for each volume
in simple or testing scenarios,
or symbolic link actual partitions as separate volumes
for more demanding, real-world setups
-- it's your choice.

In any case, volumes need to be tagged as such
in order to be recognized by NGAS.
This is done by placing a small, hidden file in the root
of the volume containing a random UID for the disk
using the ``ngasPrepareVolumeNoRoot`` utility.

For example, if the NGAS *root* directory
is under ``~/NGAS`` and a new volume called
``volume1`` is created,
it can be tagged as such::

 $ cd ~/NGAS
 $ mkdir volume1
 $ cd volume1
 $ python /path/to/ngas-sources/src/ngasUtils/src/ngasPrepareVolumeNoRoot.py --path=$PWD

Answer Yes and you're done.

To let NGAS know about your volumes check
the :ref:`config.storage_sets` configuration option.
