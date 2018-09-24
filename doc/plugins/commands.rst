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

* ``server`` is a reference to the :py:class:`ngamsServer <ngamsServer.ngamsServer.ngamsServer>` instance
  this command is running in.
  From the ``server`` object
  a pointer to the database object
  and to the configuration object
  can be obtained
  (via ``server.db`` and ``server.cfg`` respectively).
* ``request`` is an instance of ``ngamsReqProps``,
  and encapsulates information that exists in the scope of a request.
* ``http_ref`` is a reference to the HTTP connection.
  It can be used to read content from the incoming data stream,
  and send responses.

Any uncaught exception thrown by the ``handleCmd`` method
will be interpreted as an error by the NGAS server
and will generate an error status code being sent to the client,
together with an XML NGAS status document indicating the failure.

If the module returns a string,
this will be used to create an XML NGAS status document
that will contain that message,
and that will be returned to the client with a ``200`` HTTP status code.

If the module returns ``None``,
a generic success XML NGAS status document will be sent to the client.

To send other kind of replies,
please refer to the
:class:`ngamsHttpRequestHandler <ngamsServer.ngamsServer.ngamsHttpRequestHandler>` class documentation.

Registration
============

Modules implementing commands must be registered with NGAS
in order to be picked up.
This is done by listing them
in the :ref:`Commands element <config.commands>`
of the server XML configuration.
