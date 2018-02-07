Plug-ins
########

A central feature of NGAS
is its extensibility.
By writing different types of plug-ins
users can modify the default behaviour of NGAS,
add functionality that is specific to their projects,
or adjust some details for specific platforms.

Installation
============

Regardless of the type of plug-in you want to install,
NGAS must be able to find it.
Because it is not always possible
to install user-written code alongside NGAS itself
(e.g., NGAS installation could be read-only,
or writeable only by ``root``)
NGAS loads user-provided code
from any arbitrary, user-defined location.
This is indicated in the server's configuration file
with the ``PluginsPath`` variable.
For details on how this works
see the :ref:`Server <config.server>` configuration element.

Developing plug-ins
===================

Depending on the type of plug-in you want to develop,
different interfaces must be obeyed.
Also, even though all plug-ins can be *installed* in the same area,
configuring the server to actually use them is a different task,
and which is different from one plug-in type to another.

The following sub-sections detail
each plug-in type,
what is the interface they should obey,
and how to configured the server to use it.

.. toctree::

	plugins/commands
	plugins/archiving_events
	plugins/logging
	plugins/subscription_filtering
