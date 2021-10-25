# grubberbot
## Steps for setting up development
1. Clone this repository
2. Install docker https://docs.docker.com/get-docker/
   - Make sure docker-compose is installed with the command `docker-compose version`
3. Open a command line prompt and navigate to the grubberbot directory
4. Build the application with two commands
```
docker-compose up --build --detach --force-recreate --remove-orphans
docker image prune --force --all
```
5. Shut down the application when you are done
```
docker-compose down
```
