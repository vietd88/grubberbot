# grubberbot
## Set up local development environment
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
5. Install docker https://docs.docker.com/get-docker/
   - Make sure docker-compose is installed with the command `docker-compose version`

## Develop code
1. Code should go `development` -> `master` -> `production`.  Sometimes `master` will have changes that `development` does not, make sure that `development` is caught up to `master` before starting development.  
2. Make a branch from `development`, you'll make your changes on that branch. The `grubberbot/develop.py` file is used to test things, it should never actually run in production.  
3. After making a change to the branch, make sure your code doesn't break anything:
   1. Navigate to the `grubberbot` directory
   2. Check that your code style is okay
       ```
       pre-commit run --all-files
       ```
   3. Before building, keep Docker clean by removing dangling images
       ```
       docker image prune --force --all
       ```
   4. Simultaneously run unit tests and run `grubberbot/develop.py` through Docker:
       ```
       docker-compose up --build --force-recreate --remove-orphans
       ```
   5. Shut down the application when you are done
       ```
       docker-compose down
       ```
4. Finally, pull request to `development`.  NOTE: If you are using Python through Anaconda (or some other environment) an error may be raised by using git without being in that environment.  Bug is documented here https://github.com/conda-forge/pre-commit-feedstock/issues/9 The easiest fix is to use git from the environment.  For example, I use python through Anaconda and here are the steps I use:
   1. Open Anaconda prompt
   2. Activate your environment with some command like `conda activate py38`
   3. Open your git GUI from this environment.  I use GitHub Desktop, the command for me is `github` to launch the application.
   4. Do your `git` commands using the GUI that you've opened.

## Push changes
Make a pull request from `development` -> `master`.  Wait for Paul to approve the pull request.  When he does he'll push `master` -> `production`.  The `master` branch holds the current state of code, and all changes should normally go through `master` so Paul can verify them.  

If a change is urgent, users with the `mods` role can push `development` -> `production`.  This is useful if, for example, a command breaks and needs immediate attention.  Since the change has not gone through `master` it will be overwritten in the future, so make sure to also create a pull request `development` -> `master` to address this.  

Changes in `production` will not go live until the server restarts.  Users with access to grubberbot's google cloud engine can go to https://console.cloud.google.com/compute/instances?project=grubberbot and restart the VM `grubberbot-server-ubuntu`.  The bot will be down for a few minutes and then will automatically pull from `production` upon booting up.  
