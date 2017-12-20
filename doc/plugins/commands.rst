Commands
########

NGAS allows users to write their own modules
to implement new NGAS commands.

Interface
=========

The only requirement a command plug-in needs to satisfy
is to implement a method at the module level:

.. code-block:: python

   def handleCmd(server, request, http_ref)

The arguments are the following:

* ``server`` is a reference to the ``ngamsServer`` instance
  this command is running in.
  From the ``server`` object
  a pointer to the database object
  and to the configuration object
  can be obtained
  (via ``server.db`` and ``server.cfg`` respectively).
* ``request`` is an instance of ``ngamsReqProps``,
  and contains all HTTP-related information (method, headers and parameters)
  that make up the request.
* ``http_ref`` is a reference to the client connection.
  It is seldom used, but given in case your code needs it.

Registration
============

Modules implementing commands must be registered with NGAS
in order to be picked up.
This is done by listing them
in the :ref:`Commands element <config.commands>`
of the server XML configuration.
