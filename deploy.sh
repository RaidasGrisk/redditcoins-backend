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

# https://www.cloudbooklet.com/set-up-docker-on-ubuntu-18-04-lts/
# ------------ #

# copy the build files into the VM
project_id="reddit-app-308612"
vm_instance_name="db-server"

gcloud config set project "${project_id}"
gcloud compute scp docker-compose.yml "${vm_instance_name}:/home/db-deploy"

# build and run the container
gcloud compute ssh "${vm_instance_name}" \
  --command="sudo docker stop timescale pgadmin && \
             sudo docker timescale pgadmin && \
             cd /home/db-deploy/ && \
             docker-compose up -d"

# config firewall rules
gcloud compute firewall-rules create allow-pgadmin  --allow tcp:9000
gcloud compute firewall-rules create allow-postgre --allow tcp:5432
