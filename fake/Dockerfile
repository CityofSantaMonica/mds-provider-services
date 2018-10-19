FROM jupyter/scipy-notebook:137a295ff71b

# use a subdirectory of the default Notebook directory
WORKDIR /home/$NB_USER/work/mds

# the next few operations require root
USER root

# setup the app code and data directory
COPY requirements.txt requirements.txt
RUN fix-permissions /home/$NB_USER

# see https://jupyter-docker-stacks.readthedocs.io/en/latest/using/recipes.html#using-pip-install-or-conda-install-in-a-child-docker-image
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# the next few operations require root
USER root

COPY . .
RUN mkdir data && \
    fix-permissions /home/$NB_USER

# install pip packages as the notebook user
USER $NB_UID

# sets up an "executable container" for the data generation script
ENTRYPOINT ["python", "main.py"]