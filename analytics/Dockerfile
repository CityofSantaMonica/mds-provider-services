FROM jupyter/scipy-notebook:137a295ff71b

# use a subdirectory of the default Notebook directory
WORKDIR /home/$NB_USER/work/mds

USER root

RUN apt-get update && \
    apt-get install -y jq

# setup the app code and data directory
COPY requirements.txt requirements.txt

# see https://jupyter-docker-stacks.readthedocs.io/en/latest/using/recipes.html#using-pip-install-or-conda-install-in-a-child-docker-image
RUN pip install --upgrade pip && \
    pip install -r requirements.txt && \
    fix-permissions /home/$NB_USER

COPY . .

USER $NB_USER

# sets up an "executable container" for the data analytics script
ENTRYPOINT ["python", "main.py"]