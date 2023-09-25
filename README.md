Sparkify has huge user logs and song related data, it is important for their competitive advantage that this data is always availble and accessible for improved user experience and other analytics ambitions downstream. AWS's Redshift offers good Data Ware Housing solution. The following steps can be followed to setup-

    1. Creating SQL queries (sql_queries.py)
    Create all the necessary queries to create, drop, load and insert tables. These tables can then easily be accessed anywhere else.
    
    2. Create and delete IAM role, cluster, attach and detach policy (cluster_create_delete.ipynb)
    Next step is to create an IAM role and attach role policy to it, so that S3 bucket can be accessed and data can be copied from S3 to Redshift.
    Further a Redshift Cluster needs to be created.
    ** After the next two steps and any other work are done, delete the cluster, detach the policies and delete the IAM role as well.

    3. Drop and Create tables in Redshift (create_tables.py)
    Run this script to first reset (using drop) and then create tables in the Redshift data base.
    
    4. Load and Insert tables in Redshift (etl.py)
    This script is then finally run to copy/stage the data from S3. Then transform it and insert to the fact and dimensional tables created.
    
For Redshift Cluster creation a lot of the codes are taken as-is from the IaaC Excersice.