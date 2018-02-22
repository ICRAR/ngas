API
###

This is a very reduced version
of the API documentation of NGAS.
It should be helpful for users
wanting to develop plug-ins for the system.

Database access
===============

.. autoclass:: ngamsLib.ngamsDbCore.ngamsDbCore
   :members: query2, dbCursor, transaction, close

.. autoclass:: ngamsLib.ngamsDbCore.cursor2
   :members: fetch, close

.. autoclass:: ngamsLib.ngamsDbCore.transaction
   :members: execute

Server classes
==============

.. autoclass:: ngamsServer.ngamsServer.ngamsHttpRequestHandler
   :members: send_response, send_file, send_data, send_status, redirect,
             proxy_request, send_ingest_status

.. autoclass:: ngamsServer.ngamsServer.ngamsServer
   :members: db, cfg
