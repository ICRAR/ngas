Core
====

.. _commands.status:

STATUS
------

The **STATUS** command is the most basic of all. It can be used (and is used) to
confirm that an NGAS server instance is correctly running. It simply returns a
status XML document containing information about the server runtime, like its
state, disks, etc.

In particular, it can also be used to query the status
of a previous client request when given a `request_id` URL query parameter.
See :ref:`server.request_db` for more details.

OFFLINE
-------

Sends the NGAS server to the **OFFLINE** state. See :ref:`server.states`.

ONLINE
------

Sends the NGAS server to the **ONLINE** state. See :ref:`server.states`.

EXIT
----

Stops the NGAS server. The server must be in the **OFFLINE** state for the
**EXIT** command to be successful.
