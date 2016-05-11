Containers
==========

The following commands deal with *logical containers*. For an explanation on
containers see :ref:`server.logical_containers`.

For container-related commands in general, when dealing with existing containers
the following rule applies: if the container can be uniquely identified by name
the ``container_name`` parameter is enough to describe it; otherwise the
``container_id`` parameter must be given.


CCREATE
-------

Creates one or more containers but without adding files into them.

To create a single container the ``container_name`` parameter must be present,
optionally the ``parent_container_id`` can be given, and the request must be
performed using the ``GET`` method.
To create multiple containers an XML document must be sent in the request body
containing elements with a ``name`` attribute, and optionally a
``parentContainerId`` attribute at the root level. Nested elements are allowed
to create a hierarchy of containers.

.. _commands.carchive:

CARCHIVE
--------

Archives files and creates the necessary containers for them.

This command reads a MIME Multipart message from the request body. The
``Content-Disposition`` header of the multipart message contains the name of the
container. The messages inside the multipart message each contains in turn a
``Container-Disposition`` header indicating the name of the file they represent,
and their payload is the file's content. A multipart message may also contain
multipart messages inside, creating a hierarchy of containers.

.. _commands.cappend:

CAPPEND
-------

Appends an existing file into an existing container.

If using the ``GET`` method the ``file_id`` parameter must point to a file that
will be added to the container. Multiple files can also be added at once when
using the ``POST`` method and sending an XML document in the request body
consisting of a list of ``File`` elements, each with a ``FileId`` attribute in
them pointing to an existing file.

CDESTROY
--------

Destroys a single container, without removing its files.

If the optional ``recursive`` parameter is set to ``1``  the children containers
will also be removed recursively.

CREMOVE
-------

Removes an existing file from an existing container.

File specifications follow the same rules followed by :ref:`commands.cappend`.

CRETRIEVE
---------

Retrieves all the contents of a container.

See :ref:`commands.carchive` for a description of the format used by the
response body to transmit the contents of the container.

CLIST
-----

Returns a status XML document containing the container hierarchy rooted at the
specified container.
