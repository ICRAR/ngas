Archiving event handlers
########################

Apart from the built-in :ref:`server.archiving_events`,
users can provide their own code to handle them.

Interface
=========

Archiving event handler are implemented
by writing a python class with a ``handle_event`` method.

.. code-block:: python

   class MyEventHandler(object):

       def __init__(**kwargs):
           pass

       def handle_event(evt):
           pass

The class constructor should accept keyword arguments
(corresponding to the parameters set in the configuration, see below).
The class' ``handle_event`` method
accepts a unique parameter (the archiving event),
and gets invoked for each archiving event.
The event has two members, ``file_id`` and ``file_version``,
with the ID of the file just archived and its version.

.. note::

 Archiving event handlers run synchronously
 as part of the archiving process.
 Therefore it is vital they are fast,
 otherwise they can block the server.

Registration
============

Archive event handler classes must be registered with NGAS
in order to be picked up.
See :ref:`config.archivehandling`
for details on how to do this.
