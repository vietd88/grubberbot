# grubberbot
## Steps for setting up development
1. Install python
2. Clone this repository
3. Install development environment
    ```
    pip install --upgrade pip
    pip install -r requirements-dev.txt
    ```
4. Install `pre-commit`.  
   - NOTE: if `git` is not on your `PATH`, you need to add it.  For me using GitHub Desktop on Windows, `git.exe` is located at `C:\Users\<username>\AppData\Local\GitHubDesktop\app-<appversion>\resources\app\git\cmd\git.exe`  PS: AppData is a hidden folder by default.
    ```
    pre-commit install
    ```
2. Install docker https://docs.docker.com/get-docker/
   - Make sure docker-compose is installed with the command `docker-compose version`
3. Open a command line prompt and navigate to the grubberbot directory
   4. Build the application
    ```
    pre-commit run --all-files
    docker-compose up --build --force-recreate --remove-orphans
    ```
   5. In between builds make sure to remove dangling images
    ```
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

## To run the bot in production
```
docker-compose -f docker-compose-production.yml up --build --force-recreate --remove-orphans
```
