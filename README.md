# AWS Lambda RDS Database Loader

Use this function to load CSV files from any S3 location into RDS tables. Tables and columns will be auto generated if they don't exist.

## Getting Started - Lambda Execution Role
You also need to add an IAM policy as shown below to the role that AWS Lambda 
uses when it runs. Once your function is deployed, add the following policy to 
the `LambdaExecRole` to enable AWS Lambda to call SNS, use DynamoDB, write Manifest 
files to S3, perform encryption with the AWS Key Management Service. At the moment 
Lambda cannot communicate over VPC and so the RDS needs to allow all external 
connections.

```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "Stmt1424787824000",
            "Effect": "Allow",
            "Action": [
                "dynamodb:DeleteItem",
                "dynamodb:DescribeTable",
                "dynamodb:GetItem",
                "dynamodb:ListTables",
                "dynamodb:PutItem",
                "dynamodb:Query",
                "dynamodb:Scan",
                "dynamodb:UpdateItem",
                "sns:GetEndpointAttributes",
                "sns:GetSubscriptionAttributes",
                "sns:GetTopicAttributes",
                "sns:ListTopics",
                "sns:Publish",
                "sns:Subscribe",
                "sns:Unsubscribe",
                "s3:Get*",
                "s3:Put*",
                "s3:List*",
                "kms:Decrypt",
                "kms:DescribeKey",
                "kms:GetKeyPolicy"
            ],
            "Resource": [
                "*"
            ]
        }
    ]
}
```

## Getting Started - Configuration
In order to setup a lamda configuration run:
```
nodejs setup.js
```

Item | Required | Notes
:---- | :--------: | :-----
Enter the Region for the Configuration | Y | Any AWS Region from http://docs.aws.amazon.com/general/latest/gr/rande.html, using the short name (for example us-east-1 for US East 1)
Enter the S3 Bucket | Y | Bucket name will be used to lookup RDS configuration for the particular bucket.
Enter a Filename Filter Regex | N | A Regular Expression used to filter files before they are processed.
Enter the RDS Host | Y | Database host.
Enter the RDS Port | Y | Database port.
Enter the Database Name | Y | Database to use.
Enter the Schema | N | Schema to use, default: public.
Enter table prefix | N | Prefix newly created tables. This is recommended if files start with numbers.
Enter the folder depth from bucket root to use as table name. Use negative index to select from the input file | Y | Determines names for new tables. For example: /test/path/file.csv, index of 0 would create a table named *test* and index of -1 would creat a table named *file_csv*.
Should the Table be Truncated before Load? | N | Truncate table before loading new data.
Enter the Database Username | Y | Database username.
Enter the Database Password | Y | Database password.
Enter the CSV Delimiter | N | CSV delimiter, default: ,.

Configuration will be stored in DynamoDB database LambdaRDSLoaderConfig.

## Getting Started - Running
Upload the lambda function as a zip file or use node-lambda. Create an S3 watch on an S3 location. Bucket will correspond to RDS configuration tagged with such bucket name.
