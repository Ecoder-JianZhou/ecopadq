EcoPAD Queue
====
iii
Ecopad task queue. Good starting repo for various asynchronous tasks. 

After setting up the system,check the docker containers that are running.

      `# docker ps`
      
If all the six containers are not running,then there is a problem with the docker containers and check the log of the corresponding docker container which is failing to load up.
      
      `#docker logs container_name`
      
Sometimes checking the celery logs also can give the hint where the problem in our system is which is in ecopad/ecopad/celery/log folder.

      `#tail -f celery.log`
      
Everytime you make some changes in the system,don't forget to restart the system.For that go to the ecopad/ecopad/run folder and run the following commands.

      `#./docker_restart`
      
      `#./cybercom_up`
      
The ./docker_restart command kills all the docker containers and removes them from the system.

The ./cybercom_up command runs the bash script cybercom_up.sh and creates the docker containers mongo,rabbitmq and links it with the celery container.Then it creates the api container and links the memcache,mongo and rabbitmq.Finally it creates the nginx container and links the api container with it.So all the containers is linked with one another.
