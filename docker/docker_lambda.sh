# Running this in a Cloud9 instance, since it already has Docker installed on Amazon Linux 2.
# Import this script, together with the Dockerfile, python script (queue_consumer.py), and requirements.txt, into Cloud9 in the same dir.
# Then, run the script.
mkdir docker_lambda
cd docker_lambda
cp ../Dockerfile.txt Dockerfile
cp ../queue_consumer.py queue_consumer.py
cp ../requirements.txt requirements.txt

docker build -t queue-consumer .