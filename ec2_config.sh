# Install venv
sudo apt update
sudo apt install python3-venv python3-pip -y

# Create a virtual environment
python3 -m venv ~/airflow_env

# Activate the environment
source ~/airflow_env/bin/activate

# Upgrade pip inside the venv
pip install --upgrade pip

# Install required packages inside the venv
pip install apache-airflow
pip install pandas
pip install s3fs
pip install boto3
pip install requests