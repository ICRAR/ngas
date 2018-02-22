Background tasks
================

Apart from serving requests to users,
the NGAS server executes different tasks in the background
with a given period.

This section describes
the purpose of these tasks,
how they work,
and which of their aspects are configurable.

.. _bg.janitor_thread:

Janitor thread
--------------

The Janitor thread is a background task
taking care of generic, routine tasks.
Among other things,
it checks that sufficient disk space is available
for each of the configured volumes,
cleans up old data from temporary directories,
sends pending notifications,
archives rotated logfiles, and more.
Moreover, additional tasks to be carried out
by the janitor thread
can be specified by users via the configuration file
and implemented as user-provided plug-ins.

.. _bg.datacheck_thread:

Data check thread
-----------------

The data check thread is a background task
that periodically checks the integrity
of the files sitting on disk
by comparing their checksum as stored in the database
against a freshly computed checksum.
All aspects of the data check thread
are configured via the NGAS server configuration file
(see :ref:`config.datacheck_thread`).

Data checking can be expensive depending on your setup,
as it re-reads the full contents of the ingested data.
Not all systems might be able to carry out such a task;
thus data checking can be enabled or disabled.

The data check thread continuously runs
a full data check cycle
with a configurable period.
At the beginning of each data check cycle
a lists all files on disk is constructed,
then their checksums are calculated
(using the same CRC variant
that was used to archive the file)
and compared against the database-stored values.
Any checksum failures
or files found to be unregistered
are then notified.
Finally, the data check thread waits until the period finishes
to start a new cycle.

The data check thread actually uses a pool of processes
to carry out the checksum calculations,
increasing its performance
when more than one core is available in the system.
This parallel execution of checksum checking
also takes into account the volumes to which the files belong to,
reading files in parallel from different volumes when possible.

Finally, all data checking workload is fully paused
whenever the server is serving a user request.
This prevents user requests to be slowed down
due to resource exhaustion produced by the data checking processes
(in particular, CPU and disk reading).

.. _bg.cache_thread:

Cache control
-------------

The Cache control task, if enabled,
periodically removes local files from the server.
This is useful in setups
where an NGAS server acts as a buffer
to received data locally
before replicating it
to different, remote locations.
Being able to remove local files automatically
keeps the overall disk in check,
allowing users to decide
what their space needs are
depending on the buffering capabilities
needed by the system.

A number of criteria control
how and when local files are removed from NGAS.

* A per-file time limit has been reached.
  If configured, files are removed from the server
  after a given amount of time has passed
  since the file was originally archived.
* A maximum amount of storage capacity has been hit.
  When configured, files are removed
  when their total volume exceeds the specified maximum value.
  Older files are deleted first.
* A maximum number of files has been hit.
  When this option is set, files are removed
  when their total number exceeds the configured limit.
  Older files are deleted first.
* A user-provided plug-in makes the decision.
  Users can write *ad-hoc* code to decide
  whether particular files should be deleted (or not).

More than one rule can be active at a given time,
in which case they are processed in the order given above.
On top of that, if the Subscription service is enabled
files will only be eligible for deletion
after they are successfully transmitted to all their subscribers
(this cannot be overridden).
