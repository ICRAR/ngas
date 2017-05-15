############
Introduction
############

The Next Generation Archive System (NGAS) is a very feature rich, archive
handling and management system.
In its core it is a HTTP based object storage system. It can be deployed
on single small servers, or in globally distributed clusters.

Some of its main features are:

 * Basic archiving and retrieval of data
 * Data checking via various checksum methods
 * Server-side data compression and filtering
 * Automatic mirroring of data
 * Clustering and swarming
 * Disk tracking and offline data transfer
 * High customisation via user-provided plug-ins

NGAS is written in Python 2.7, making it highly portable.
There are a few dependencies on C libraries,
which may restrict the ability to install it
on some of the more exotic platforms.
