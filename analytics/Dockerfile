FROM jupyter/scipy-notebook:137a295ff71b

# use a subdirectory of the default Notebook directory
WORKDIR /home/$NB_USER/work/mds

USER root

# setup the app code and data directory
COPY requirements.txt requirements.txt

# see https://jupyter-docker-stacks.readthedocs.io/en/latest/using/recipes.html#using-pip-install-or-conda-install-in-a-child-docker-image
RUN pip install --upgrade pip && \
    pip install -r requirements.txt && \
    fix-permissions /home/$NB_USER

USER $NB_USER

COPY . .

# sets up an "executable container" for the data analytics script
ENTRYPOINT ["python", "main.py"]