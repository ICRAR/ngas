Logfile handling
################

As explained in :ref:`server.logging`,
users can write their own logfile handling plug-ins.
These plug-ins will be invoked each time a logfile is rotated,
which happens after a fixed amount of time,
and every time the server is started.

Interface
=========

Logfile handling is implemented
by writing a python module with a ``run`` method.

.. code-block:: python

   def run(srv, fname):
       pass

The ``srv`` argument is an :py:class:`ngamsServer <ngamsServer.ngamsServer.ngamsServer>` object,
and the ``fname`` is the path to the rotated logfile.
Logfile handler plug-ins run asynchronously
as part of the :ref:`janitor thread <bg.janitor_thread>`,
and therefore it is acceptable
that they take some time to run.

Registration
============

Logfile handling plug-ins must be registered with NGAS
in order to be picked up.
See :ref:`config.log`
for details on how to do this.
