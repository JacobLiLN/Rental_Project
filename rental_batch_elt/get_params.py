import boto3, os, yaml

ssm = boto3.client('ssm', region_name='us-east-1')
get = lambda name: ssm.get_parameter(Name=name, WithDecryption=True)['Parameter']['Value']

profile = {
    'rental_batch_elt': {
        'outputs':{
            'dev': {
                'type': 'snowflake',
                'account': get('/myapp/snowflake_account'),
                'user': get('/myapp/snowflake_user'),
                'password': get('/myapp/snowflake_password'),
                'role': 'ACCOUNTADMIN',
                'database': get('/myapp/snowflake_database'),
                'warehouse': get('/myapp/snowflake_warehouse'),
                'schema': get('/myapp/snowflake_schema')
            }
        },
        'target': 'dev'
    }
}

os.makedirs('/root/.dbt', exist_ok=True)
with open('/root/.dbt/profiles.yml','w') as f:
    yaml.dump(profile, f, default_flow_style=False)