This procedure will:
* Create and bring up the required AWS instances
* Wait until they are fully operational, and
* Perform a ``hl.operations_deploy`` on the instances.

On top of the normal fabric variables used by ``hl.user_deploy`` and
``hl.operations_deploy`` the following additional variables control the
AWS-related aspects of the script:

+-----------------------------+--------------------------------------+-------------------+
| Variable                    | Description                          | Default value     |
+=============================+======================================+===================+
| AWS_PROFILE                 | | The profile to use when connecting | | ``NGAS``        |
|                             | | to AWS                             |                   |
+-----------------------------+--------------------------------------+-------------------+
| AWS_REGION                  | | The AWS region to connect to       | | ``us-east-1``   |
+-----------------------------+--------------------------------------+-------------------+
| AWS_KEY_NAME                | | The private SSH key to be used to  | | ``icrar_ngas``  |
|                             | | create the instances, and later to |                   |
|                             | | connect to them                    |                   |
+-----------------------------+--------------------------------------+-------------------+
| AWS_AMI_NAME                | | The name associated to an AMI      | | ``Amazon``      |
|                             | | (from a predetermined set of AMI   |                   |
|                             | | IDs) which will be used to create  |                   |
|                             | | the instance                       |                   |
+-----------------------------+--------------------------------------+-------------------+
| AWS_INSTANCES               | | The number of instances to create  | | ``1``           |
+-----------------------------+--------------------------------------+-------------------+
| AWS_INSTANCE_TYPE           | | The type of instances to create    | | ``t1.micro``    |
+-----------------------------+--------------------------------------+-------------------+
| AWS_INSTANCE_NAME           | | The name of instances to create    | | ``NGAS_<rev>``  |
+-----------------------------+--------------------------------------+-------------------+
| AWS_SEC_GROUP               | | The name of the security group to  | | ``NGAS``        |
|                             | | attach to the instances (will be   |                   |
|                             | | created if it doesn't exist)       |                   |
+-----------------------------+--------------------------------------+-------------------+
| AWS_ELASTIC_IPS             | | A comma-separated list of public   | | Not specified   |
|                             | | IPs to associate with the new      |                   |
|                             | | instances, if specified.           |                   |
+-----------------------------+--------------------------------------+-------------------+
