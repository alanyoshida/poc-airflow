load('ext://helm_resource', 'helm_resource', 'helm_repo')
load('ext://helm_remote', 'helm_remote')

# Install Airflow
helm_remote('airflow',
repo_url='https://airflow.apache.org',
version='1.13.1',
values=['./charts/airflow/values.yaml'],
allow_duplicates=True)


# k8s_resource('postgresql', port_forwards=["5432:5432"])