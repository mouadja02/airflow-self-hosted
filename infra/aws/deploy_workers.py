"""Deploy project-scoped workers; build the image in AWS to avoid Pi uploads.

Run inside the Airflow image with the repository at /workspace and the dedicated
AWS profile mounted read-only. Secrets are copied directly into Secrets Manager;
only resource identifiers are written to deployment-state.json.
"""

import argparse
import hashlib
import io
import json
from pathlib import Path
import time
import zipfile

import boto3
from botocore.exceptions import ClientError


ROOT = Path(__file__).resolve().parents[2]
PREFIX = 'airflow-bitcoin-'
ACCOUNT = '434740914108'
REGION = 'eu-west-3'
BOUNDARY = f'arn:aws:iam::{ACCOUNT}:policy/AirflowBitcoinWorkerBoundary'


def log(message):
    print(message, flush=True)


def statement(actions, resources):
    return {'Effect': 'Allow', 'Action': actions, 'Resource': resources}


def policy(*statements):
    return {'Version': '2012-10-17', 'Statement': list(statements)}


def ensure_role(iam, name, service, permissions):
    try:
        role = iam.get_role(RoleName=name)['Role']
        if role.get('PermissionsBoundary', {}).get('PermissionsBoundaryArn') != BOUNDARY:
            raise RuntimeError(f'Refusing to modify existing role without the worker boundary: {name}')
    except iam.exceptions.NoSuchEntityException:
        trust = policy({'Effect': 'Allow', 'Principal': {'Service': service}, 'Action': 'sts:AssumeRole'})
        role = iam.create_role(RoleName=name, PermissionsBoundary=BOUNDARY,
            AssumeRolePolicyDocument=json.dumps(trust), Tags=[{'Key': 'Project', 'Value': 'airflow-bitcoin'}])['Role']
    iam.put_role_policy(RoleName=name, PolicyName='airflow-bitcoin-project', PolicyDocument=json.dumps(permissions))
    return role['Arn']


def ensure_secret(client, name, value=None):
    try:
        arn = client.describe_secret(SecretId=name)['ARN']
    except client.exceptions.ResourceNotFoundException:
        arn = client.create_secret(Name=name, Tags=[{'Key': 'Project', 'Value': 'airflow-bitcoin'}])['ARN']
    if value is not None:
        client.put_secret_value(SecretId=arn, SecretString=json.dumps(value))
    return arn


def source_archive():
    files = {name: (ROOT / 'functions/bitcoin_workers' / name).read_bytes()
             for name in ('Dockerfile', 'requirements.txt', 'worker.py', 'test_worker.py')}
    files['bitcoin_indicators.py'] = (ROOT / 'dags/bitcoin_indicators.py').read_bytes()
    files['buildspec.yml'] = b'''version: 0.2
phases:
  pre_build:
    commands:
      - aws ecr get-login-password --region "$AWS_DEFAULT_REGION" | docker login --username AWS --password-stdin "$IMAGE_REPO_URI"
  build:
    commands:
      - docker build -t "$IMAGE_REPO_URI:$IMAGE_TAG" .
      - docker run --rm --user 10001:10001 --entrypoint python "$IMAGE_REPO_URI:$IMAGE_TAG" -m unittest -v test_worker
  post_build:
    commands:
      - test "$CODEBUILD_BUILD_SUCCEEDING" = 1
      - docker push "$IMAGE_REPO_URI:$IMAGE_TAG"
'''
    digest = hashlib.sha256(b''.join(name.encode() + data for name, data in sorted(files.items()))).hexdigest()[:20]
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w', zipfile.ZIP_DEFLATED) as archive:
        for name, data in files.items():
            archive.writestr(name, data)
    return digest, buffer.getvalue()


def copy_production_secrets(manager):
    import logging
    import os
    logging.getLogger('snowflake.connector').setLevel(logging.WARNING)
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
    parameters = SnowflakeHook(snowflake_conn_id='snowflake_default')._get_conn_params
    if callable(parameters):
        parameters = parameters()
    allowed = {'account', 'user', 'password', 'warehouse', 'database', 'schema', 'role', 'authenticator'}
    snowflake = {key: value for key, value in parameters.items() if key in allowed and value}
    github = {'username': os.environ.get('GITHUB_USERNAME', 'mouadja02'),
        'repository': os.environ.get('GITHUB_REPO_BTC_HOURLY', 'bitcoin-technical-indicators-dataset'),
        'token': os.environ['GITHUB_TOKEN'], 'email': 'mouadpro02@gmail.com'}
    if not snowflake.get('password'):
        raise RuntimeError('The configured service connection has no password; production auth needs explicit setup')
    return ensure_secret(manager, PREFIX + 'snowflake', snowflake), ensure_secret(manager, PREFIX + 'github', github)


def deploy(session):
    identity = session.client('sts').get_caller_identity()
    if identity['Account'] != ACCOUNT or not identity['Arn'].endswith(':user/airflow-bitcoin-deployer'):
        raise RuntimeError('Use the dedicated airflow-bitcoin-deployer profile in the expected account')
    iam = session.client('iam')
    iam.get_policy(PolicyArn=BOUNDARY)
    ecr, s3, build = (session.client(name) for name in ('ecr', 's3', 'codebuild'))
    manager, cfn = (session.client(name) for name in ('secretsmanager', 'cloudformation'))
    log('Validated dedicated deployer and worker boundary')
    repo_name = PREFIX + 'workers'
    try:
        repo = ecr.describe_repositories(repositoryNames=[repo_name])['repositories'][0]
    except ecr.exceptions.RepositoryNotFoundException:
        repo = ecr.create_repository(repositoryName=repo_name,
            imageScanningConfiguration={'scanOnPush': True}, tags=[{'Key': 'Project', 'Value': 'airflow-bitcoin'}])['repository']
    ecr.set_repository_policy(repositoryName=repo_name, policyText=json.dumps(policy({
        'Effect': 'Allow', 'Principal': {'Service': 'lambda.amazonaws.com'},
        'Action': ['ecr:BatchGetImage', 'ecr:GetDownloadUrlForLayer'],
        'Condition': {'StringLike': {'aws:sourceArn': f'arn:aws:lambda:{REGION}:{ACCOUNT}:function:{PREFIX}*'}}})))
    ecr.put_lifecycle_policy(repositoryName=repo_name, lifecyclePolicyText=json.dumps({'rules': [{
        'rulePriority': 1, 'description': 'Retain recent worker images',
        'selection': {'tagStatus': 'any', 'countType': 'imageCountMoreThan', 'countNumber': 5},
        'action': {'type': 'expire'}}]}))
    bucket = f'{PREFIX}build-{ACCOUNT}-{REGION}'
    try:
        s3.head_bucket(Bucket=bucket, ExpectedBucketOwner=ACCOUNT)
    except ClientError as exc:
        if exc.response['Error']['Code'] not in ('404', 'NoSuchBucket', 'NotFound'):
            raise
        s3.create_bucket(Bucket=bucket, CreateBucketConfiguration={'LocationConstraint': REGION})
    s3.put_public_access_block(Bucket=bucket, PublicAccessBlockConfiguration={
        'BlockPublicAcls': True, 'IgnorePublicAcls': True, 'BlockPublicPolicy': True, 'RestrictPublicBuckets': True})
    s3.put_bucket_encryption(Bucket=bucket, ServerSideEncryptionConfiguration={
        'Rules': [{'ApplyServerSideEncryptionByDefault': {'SSEAlgorithm': 'AES256'}}]})
    s3.put_bucket_lifecycle_configuration(Bucket=bucket, LifecycleConfiguration={
        'Rules': [{'ID': 'expire-build-sources', 'Status': 'Enabled', 'Filter': {'Prefix': 'source/'}, 'Expiration': {'Days': 30}}]})
    logs = [f'arn:aws:logs:{REGION}:{ACCOUNT}:log-group:/aws/{kind}/{PREFIX}*' for kind in ('lambda', 'codebuild')]
    builder_role = ensure_role(iam, PREFIX + 'builder', 'codebuild.amazonaws.com', policy(
        statement(['logs:CreateLogGroup', 'logs:CreateLogStream', 'logs:PutLogEvents'], logs),
        statement(['s3:GetBucketLocation', 's3:ListBucket'], f'arn:aws:s3:::{bucket}'),
        statement(['s3:GetObject', 's3:GetObjectVersion'], f'arn:aws:s3:::{bucket}/*'),
        statement('ecr:GetAuthorizationToken', '*'),
        statement(['ecr:BatchCheckLayerAvailability', 'ecr:BatchGetImage', 'ecr:CompleteLayerUpload',
            'ecr:GetDownloadUrlForLayer', 'ecr:InitiateLayerUpload', 'ecr:PutImage', 'ecr:UploadLayerPart'], repo['repositoryArn'])))
    snowflake_secret, github_secret = copy_production_secrets(manager)
    base_permissions = [statement(['logs:CreateLogGroup', 'logs:CreateLogStream', 'logs:PutLogEvents'], logs)]
    calculate_role = ensure_role(iam, PREFIX + 'calculate', 'lambda.amazonaws.com', policy(
        *base_permissions, statement('secretsmanager:GetSecretValue', snowflake_secret)))
    publish_role = ensure_role(iam, PREFIX + 'publish', 'lambda.amazonaws.com', policy(
        *base_permissions, statement('secretsmanager:GetSecretValue', [snowflake_secret, github_secret])))
    function_arns = {action: f'arn:aws:lambda:{REGION}:{ACCOUNT}:function:{PREFIX}{action}' for action in ('calculate', 'publish')}
    state_role = ensure_role(iam, PREFIX + 'orchestrator', 'states.amazonaws.com', policy(
        statement('lambda:InvokeFunction', list(function_arns.values()))))
    image_tag, archive = source_archive()
    key = f'source/{image_tag}.zip'
    s3.put_object(Bucket=bucket, Key=key, Body=archive, ServerSideEncryption='AES256')
    project = PREFIX + 'workers-image'
    configuration = dict(name=project, source={'type': 'S3', 'location': f'{bucket}/{key}'},
        artifacts={'type': 'NO_ARTIFACTS'}, serviceRole=builder_role, timeoutInMinutes=30,
        environment={'type': 'LINUX_CONTAINER', 'image': 'aws/codebuild/standard:7.0',
            'computeType': 'BUILD_GENERAL1_SMALL', 'privilegedMode': True,
            'environmentVariables': [{'name': 'IMAGE_REPO_URI', 'value': repo['repositoryUri']}, {'name': 'IMAGE_TAG', 'value': image_tag}]},
        logsConfig={'cloudWatchLogs': {'status': 'ENABLED', 'groupName': '/aws/codebuild/' + project}})
    if build.batch_get_projects(names=[project])['projects']:
        build.update_project(**configuration)
    else:
        time.sleep(10)  # Allow the new service role and inline policy to propagate.
        build.create_project(**configuration)
    try:
        image = ecr.describe_images(repositoryName=repo_name, imageIds=[{'imageTag': image_tag}])['imageDetails'][0]
    except ecr.exceptions.ImageNotFoundException:
        log(f'Building in AWS CodeBuild; only {len(archive):,} source bytes uploaded from the Pi')
        current = build.start_build(projectName=project)['build']
        while current['buildStatus'] == 'IN_PROGRESS':
            log(f'CodeBuild {current["id"]}: {current.get("currentPhase", "starting")}')
            time.sleep(20)
            current = build.batch_get_builds(ids=[current['id']])['builds'][0]
        if current['buildStatus'] != 'SUCCEEDED':
            raise RuntimeError(f'CodeBuild {current["buildStatus"]}: {current.get("logs", {}).get("deepLink", "")}; {current.get("phases", [])}')
        image = ecr.describe_images(repositoryName=repo_name, imageIds=[{'imageTag': image_tag}])['imageDetails'][0]
    image_uri = repo['repositoryUri'] + '@' + image['imageDigest']
    resources = {}
    for action in ('calculate', 'publish'):
        resources[action.title() + 'Function'] = {'Type': 'AWS::Lambda::Function', 'Properties': {
            'FunctionName': PREFIX + action, 'PackageType': 'Image', 'Code': {'ImageUri': image_uri},
            'Role': calculate_role if action == 'calculate' else publish_role,
            'Timeout': 900, 'MemorySize': 3072, 'EphemeralStorage': {'Size': 2048},
            'Architectures': ['x86_64'],
            'Environment': {'Variables': {'SNOWFLAKE_SECRET_ARN': snowflake_secret, 'GITHUB_SECRET_ARN': github_secret}}}}
    states = {'Dispatch': {'Type': 'Choice', 'Choices': [
        {'Variable': '$.action', 'StringEquals': action, 'Next': action.title()} for action in ('calculate', 'publish')], 'Default': 'InvalidAction'},
        'InvalidAction': {'Type': 'Fail', 'Error': 'InvalidWorkerAction'}}
    for action in ('calculate', 'publish'):
        states[action.title()] = {'Type': 'Task', 'Resource': 'arn:aws:states:::lambda:invoke',
            'Parameters': {'FunctionName': function_arns[action], 'Payload.$': '$'},
            'OutputPath': '$.Payload', 'TimeoutSeconds': 930, 'End': True,
            'Retry': [{'ErrorEquals': ['Lambda.ServiceException', 'Lambda.SdkClientException', 'Lambda.TooManyRequestsException'],
                'IntervalSeconds': 2, 'MaxAttempts': 3, 'BackoffRate': 2}]}
    machine_name = PREFIX + 'workers'
    machine_arn = f'arn:aws:states:{REGION}:{ACCOUNT}:stateMachine:{machine_name}'
    resources['WorkerStateMachine'] = {'Type': 'AWS::StepFunctions::StateMachine',
        'DependsOn': ['CalculateFunction', 'PublishFunction'], 'Properties': {
            'StateMachineName': machine_name, 'RoleArn': state_role,
            'Definition': {'StartAt': 'Dispatch', 'States': states}}}
    template = {'AWSTemplateFormatVersion': '2010-09-09', 'Description': 'Bitcoin calculation and publication cloud workers', 'Resources': resources}
    stack = PREFIX + 'workers'
    try:
        existing = cfn.describe_stacks(StackName=stack)['Stacks'][0]
    except ClientError as exc:
        if 'does not exist' not in str(exc):
            raise
        existing = None
    log('Deploying Lambda functions and Step Functions state machine')
    if existing:
        try:
            cfn.update_stack(StackName=stack, TemplateBody=json.dumps(template))
            waiter = cfn.get_waiter('stack_update_complete')
        except ClientError as exc:
            if 'No updates are to be performed' not in str(exc):
                raise
            waiter = None
    else:
        cfn.create_stack(StackName=stack, TemplateBody=json.dumps(template), Tags=[{'Key': 'Project', 'Value': 'airflow-bitcoin'}])
        waiter = cfn.get_waiter('stack_create_complete')
    if waiter:
        waiter.wait(StackName=stack, WaiterConfig={'Delay': 15, 'MaxAttempts': 80})
    log_client = session.client('logs')
    for group in ['/aws/codebuild/' + project, *('/aws/lambda/' + PREFIX + action for action in ('calculate', 'publish'))]:
        try:
            log_client.create_log_group(logGroupName=group)
        except log_client.exceptions.ResourceAlreadyExistsException:
            pass
        log_client.put_retention_policy(logGroupName=group, retentionInDays=14)
    runtime_name = PREFIX + 'runtime'
    try:
        user = iam.get_user(UserName=runtime_name)['User']
        if user.get('PermissionsBoundary', {}).get('PermissionsBoundaryArn') != BOUNDARY:
            raise RuntimeError('Existing runtime user does not have the expected boundary')
    except iam.exceptions.NoSuchEntityException:
        iam.create_user(UserName=runtime_name, PermissionsBoundary=BOUNDARY, Tags=[{'Key': 'Project', 'Value': 'airflow-bitcoin'}])
    iam.put_user_policy(UserName=runtime_name, PolicyName='airflow-bitcoin-executions', PolicyDocument=json.dumps(policy(
        statement('states:StartExecution', machine_arn),
        statement(['states:DescribeExecution', 'states:GetExecutionHistory'], f'arn:aws:states:{REGION}:{ACCOUNT}:execution:{machine_name}:*'))))
    runtime_secret = ensure_secret(manager, PREFIX + 'runtime-credentials')
    try:
        manager.get_secret_value(SecretId=runtime_secret)
    except manager.exceptions.ResourceNotFoundException:
        if iam.list_access_keys(UserName=runtime_name)['AccessKeyMetadata']:
            raise RuntimeError('Runtime user already has keys but no stored credential; recover or rotate that project user explicitly')
        key_info = iam.create_access_key(UserName=runtime_name)['AccessKey']
        manager.put_secret_value(SecretId=runtime_secret, SecretString=json.dumps({
            'aws_access_key_id': key_info['AccessKeyId'], 'aws_secret_access_key': key_info['SecretAccessKey'], 'region_name': REGION}))
    state = {'account': ACCOUNT, 'region': REGION, 'image_uri': image_uri, 'state_machine_arn': machine_arn,
        'runtime_secret_arn': runtime_secret, 'functions': function_arns, 'build_project': project,
        'source_bucket': bucket, 'snowflake_secret_arn': snowflake_secret, 'github_secret_arn': github_secret}
    (ROOT / 'infra/aws/deployment-state.json').write_text(json.dumps(state, indent=2) + '\n')
    log('Deployment complete: ' + json.dumps(state))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--profile', default='airflow-bitcoin-deployer')
    args = parser.parse_args()
    deploy(boto3.Session(profile_name=args.profile, region_name=REGION))
