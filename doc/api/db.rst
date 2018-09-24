Database access
###############

.. contents::

Core database access
====================

These are the core classes implementing the core database access logic,
including dealing with connections, cursors and transactions.
For the higher-level database object
offered by the :py:class:`ngamsServer.ngamsServer.ngamsServer` class
please see :ref:`api.db.hl`.

.. autoclass:: ngamsLib.ngamsDbCore.ngamsDbCore
   :members: query2, dbCursor, transaction, close

.. autoclass:: ngamsLib.ngamsDbCore.cursor2
   :members: fetch, close

.. autoclass:: ngamsLib.ngamsDbCore.transaction
   :members: execute


.. _api.db.hl:

Higher-level abstractions
=========================

.. autofunction:: ngamsLib.ngamsDb.from_config

.. autoclass:: ngamsLib.ngamsDb.ngamsDb
   :inherited-members:
