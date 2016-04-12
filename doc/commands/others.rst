Others
======

SUBSCRIBE
---------

The Data Subscription Service of NGAS makes it possible to synchronize a full or partial set of data
files to remote Data Subscribers which can be other NGAS nodes. A client subscribing for data is referred to as a Data Subscriber.
An NGAS Server, which delivers data to such a Subscriber, is referred to as a Data Provider.

A Data Subscriber can specify to receive data files from a certain point in the past to the present day.
If the time is not defined then only newly archived files will be delivered to the subscriber.
It is also possible for the Data Subscriber to specify a Filter Plug-In which is applied to a data file
before it is delivered.

The client subscribes itself by supplying a Subscriber URL to the NGAS Data Provider.
NGAS delivers data to the client by performing a HTTP POST on the Subscriber URL.
The client must be ready to handle data file HTTP POST requests from the Data Provider.
Any HTTP based server can be used, from a simple customized implementation to an existing and widely used server like Apache.

An NGAS Server can be configured to subscribe to another NGAS Server. In this case the
Subscriber URL should be the URL used when performing an Archive Push Request::

 http://<host>:<port>/QARCHIVE

Note that ``NgamsCfg.SubscriptionDef:Enable`` must be set to ``1`` to enable subscription.
It is possible to instruct an NGAS Server to un-subscribe itself automatically
when it goes Offline ``NgamsCfg.SubscriptionDef:AutoUnsubscribe`` set to ``1``.

When a client has first subscribed itself to a certain type of file,
NGAS guarantees that all files of that type (with the matching time constraint)
will be delivered to the client. If it is impossible to deliver a file
e.g. the client has terminated or the network is down,
NGAS maintains a subscription back-log which will try to periodically deliver the files to the client.
``NgamsCfg.SubscriptionDef:SuspensionTime`` determines how often the NGAS server will process the back-log.

It is possible to specify an expiration time indicating for how long
files should be kept in the back-log ``NgamsCfg.SubscriptionDef.BackLogExpTime``.
Files residing longer than the expiration time will be deleted and thus never delivered.
The name of the subscription back-log is defined by the parameter ``<NgamsCfg.Server:BackLogBufferDirectory>/subscr-back-log``.

A simple scheme has been implemented to avoid the scenario where the same data file is delivered to the same subscriber multiple times.
This scheme is based on recording the ingestion date for the last file delivered. i.e., only files with a more
recent ingestion date will be taken into account. This remembered ‘last ingestion date’
for each subscriber will be reset if a start date for the subscription ‘older’ than this date is specified by a client.

SUBSCRIBE is the command used by Data Subscribers to subscribe to an NGAS server.

**Parameters**

- ``subscr_id``: Subscription ID that should be unique.
- ``url``: The URL to which the archived file(s) will be delivered.
- ``concurrent_threads``: Number of simultaneous file data delivery threads.
- ``start_date``: Date from which the data to deliver is taken into account. If not specified the time when the SUBSCRIBE command was received is taken as start date.
- ``priority``: Priority for delivering data to this Data Subscriber. The lower the number, the higher the priority. Clients with a higher priority, get more CPU time in connection with the data delivery.
- ``filter_plug_in``: Name of a Filter Plug-In to invoke on the file(s).
- ``plug_in_pars``: A set of parameters to transfer to the Filter Plug-In when it is invoked.

Example::

 curl http://localhost:8000/SUBSCRIBE?subscr_id=TEST&url=http://localhost:8889/QARCHIVE&priority=1&start_date=2016-03-01T00:00:00.000&concurrent_threads=4

This example has two NGAS instances running on the same node. One instance is bound to port ``8000`` and the other to port ``8889``.
The subscriber is telling the NGAS instance on ``8000`` to deliver all the files it ingested from the date ``2016-03-01`` to the present day using ``4`` concurrent delivery threads at the highest priority to NGAS instance ``8889``.


UNSUBSCRIBE
-----------

Used by Data Subscribers to unsubscribe to a previously established subscription.
If NGAS holds a back-log of data files for that subscription, that back-log will be cleared and data delivery will stop.

**Parameters**

- ``subscr_id``: Subscription ID to unsubscribe.
