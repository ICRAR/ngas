# NGAS

[![Docs](https://readthedocs.org/projects/ngas/badge/?version=latest)](https://ngas.readthedocs.io/en/latest/)

The Next Generation Archive System (NGAS) is a very feature rich, archive
handling and management system.
In its core it is a HTTP based object storage system. It can be deployed
on single small servers, or in globally distributed clusters.

NGAS is written in Python 2.7, and thus highly portable. The implementation
is pure Python, but there are a few dependencies on C libraries as well
and that may restrict the ability to install it on some of the more exotic
platforms.
It is possible to run more than one server on a single host and it is
possible to run many servers across hundreds of nodes as well as across
various sites. The more advanced features allow mirroring of sites running
independent NGAS clusters, but it is also possible to run multiple clusters
against a central database.

# Installation

Please refer to the Installation section of our documentation for instructions
on how to compile and install NGAS either manually or automatically.

# Documentation

Our documentation is [online](https://ngas.readthedocs.io/).

# History

The system has been originally developed and used extensively at the
European Southern Observatory to archive the data at the observatories in
Chile and mirror the data to the headquarters in Germany. It has also been
deployed at the NRAO in Socorro and Charlottesville and the Atacama Large
Millimeter/Submillimeter Array (ALMA) is using the system to collect the data
directly at the control site in the Chilean Andes and then distribute the data
to Santiago and further on to the ALMA regional centers in the US, Japan and
Germany. NGAS is controlling millions of files all around the world and it
is scaling very well. The version of NGAS delivered in this distribution is
a branch of the original ALMA version. It had been further developed and
adopted to deal with the much higher data rate of the Murchison Widefield
Array (MWA) and quite some work went into the installation and automatic test
features.

# Copyright

Please see the COPYRIGHT file for information on copyright, and the AUTHORS and
ACKNOWLEDGMENTS files for information about authors and the story of NGAS.
