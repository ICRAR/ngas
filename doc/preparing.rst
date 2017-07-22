Setting up an NGAS instance
===========================

Create an NGAS root directory
-----------------------------

NGAS's *root* is the top-level directory
that will be used to store all its internal files,
including the data being stored.

If you used the `fabric-based installation <_inst.fabric>` procedure
then the NGAS *root* directory will have been created already
under ``~/NGAS`` of the user hosting the NGAS installation
(or somewhere different if indicated via the ``NGAS_ROOT`` fabric variable)

Setup volumes
-------------

Inside the NGAS *root* directory
*volumnes* should exist
where the data will be stored.
You can either create directories for each volume,
or symbolic link actual partitions as separate volumes
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
the `config.volumes` configuration option.
