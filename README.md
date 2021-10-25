# grubberbot
## Steps for setting up development
1. Install python
2. Clone this repository
3. Install development environment
```
pip install -r requirements-dev.txt
pre-commit install
```
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
6. If you are using Python through Anaconda (or some other environment) an error may be raised by using git without being in that environment.  Bug is documented here https://github.com/conda-forge/pre-commit-feedstock/issues/9 The easiest fix is to use git from the environment.  For example, I use python through Anaconda and here are the steps I use:
   1. Open Anaconda prompt
   2. Activate your environment with some command like `conda activate py38`
   3. Open your git GUI from this environment.  I use GitHub Desktop, the command for me is `github` to launch the application.
   4. Do your `git` commands using the GUI that you've opened.
