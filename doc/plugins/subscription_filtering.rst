Subscription filtering
######################

NGAS allows users to write their own modules
to provide subscription filtering logic.

Interface
=========

The only requirement a subscription filtering plug-in has
is to implement one method at the module level
with the same name of the module itself:

.. code-block:: python

   def my_plugin_name(server, plugin_pars, file_name, file_id, file_version)

``my_plugin_name`` needs to match the name of the module;
in other words, the file containing it must be called
``my_plugin_name.py``.

The arguments are the following:

* ``server`` is a reference to the ``ngamsServer`` instance
  this command is running in.
  From the ``server`` object
  a pointer to the database object
  and to the configuration object
  can be obtained
  (via ``server.db`` and ``server.cfg`` respectively).
* ``plugin_pars`` is the comma-separated, ``key=value`` pairs of parameters
  as stored by the subscription in the database.
* ``file_name``, ``file_id`` and ``file_version`` are the name, ID and version
  of the file delivery being assessed.

The method should return ``True`` if the file should be delivered,
and ``False`` otherwise.

Please note that filtering is done on a per-file basis.
Calculating the list of files that will be fed into the plug-in
is outside of the scope of this plug-in, and depends
on the subscription settings, like its start date.

Registration
============

Modules implementing commands must be registered with NGAS
in order to be picked up.
This is done by listing them
in the :ref:`Commands element <config.commands>`
of the server XML configuration.
