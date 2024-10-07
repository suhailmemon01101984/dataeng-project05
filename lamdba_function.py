#create the lambda function using the aws console UI. python 3.12 as runtime, x86_64 as architecture, change the default execution role to
#suhailmemon84-lambda-s3-glue-access.
#once the function is created, go under configuration, then general configuration then click edit and update memory and ephemeral storage both to 512 mb
#and update timeout to 5 min 3 seconds.
#finally go under the code tab, scroll down and then go in the section: Layers and click on add a layer and add the layer: aws layers --> awssdkpandas-python312
#details about the role: suhailmemon84-lambda-s3-glue-access. create this role under IAM and grant it amazons3fullaccess awsconsolefullaccess and awsglueservicerole
