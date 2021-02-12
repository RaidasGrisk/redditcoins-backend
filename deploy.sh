# ------------ #
# should put the init part and dep installation into a separate file
# copy it over to the VM and execute the file

# # init and install dependencies
# sudo apt-get update
# sudo apt-get upgrade

# # install docker
# sudo apt install apt-transport-https ca-certificates curl gnupg-agent software-properties-common
# curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
# sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
# sudo apt install docker-ce docker-ce-cli containerd.io

# # https://docs.docker.com/compose/install/
# sudo curl -L "https://github.com/docker/compose/releases/download/1.27.4/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
# sudo chmod +x /usr/local/bin/docker-compose

# ------------ #

# copy the build files into the VM
project_name="trade-demo-302513"
vm_instance_name="db-server"

gcloud config set project "${project_name}"
gcloud compute scp docker-compose.yaml "${vm_instance_name}:/home/db-deploy"

# build and run the container
gcloud compute ssh "${vm_instance_name}" \
  --command="sudo docker stop mongo-dev mongo-express && \
             sudo docker rm mongo-dev mongo-express && \
             cd /home/db-deploy/ && \
             docker-compose up -d"

# config firewall rules
gcloud compute firewall-rules create allow-mongodb --allow tcp:27017
gcloud compute firewall-rules create allow-mongodb-express --allow tcp:8081

# docker stop mongo-dev mongo-express
# docker rm mongo-dev mongo-express
# docker-compose up -d
