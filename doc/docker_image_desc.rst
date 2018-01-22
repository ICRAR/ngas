This task will:

* Create an initial Docker image with SSH on it
* Start a container using that image
* Perform a ``hl.operations_deploy`` on the container
* Perform some cleanups on the container, including removing the ``NGAS_ROOT`` directory
* Commit the container and create the final Docker image

On top of the normal fabric variables used by ``hl.user_deploy`` and
``hl.operations_deploy`` the following additional variables control the
Docker-related aspects of the task:

+-----------------------------+--------------------------------------+-------------------+
| Variable                    | Description                          | Default value     |
+=============================+======================================+===================+
| DOCKER_KEEP_NGAS_ROOT       | | If specified, the NGAS root        | | Not specified   |
|                             | | directory will still be present in |                   |
|                             | | the final image                    |                   |
+-----------------------------+--------------------------------------+-------------------+
| DOCKER_KEEP_NGAS_SRC        | | If specified, the NGAS source      | | Not specified   |
|                             | | directory will still be present in |                   |
|                             | | the final image                    |                   |
+-----------------------------+--------------------------------------+-------------------+
| DOCKER_IMAGE_REPOSITORY     | | The repository for the final image | | ``icrar/ngas``  |
|                             | | produced by this task              |                   |
+-----------------------------+--------------------------------------+-------------------+
