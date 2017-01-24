FROM continuumio/miniconda:4.1.11

# make shell directories so that other commands work
RUN mkdir -p /usr/src/app/yarnitor/static
WORKDIR /usr/src/app

# create a conda environment including pytest
RUN conda update --all python=3.5 pytest -c conda-forge

# copy in the backend requirements so that we benefit from caching
COPY requirements.txt /usr/src/app
# most are not available on conda yet so just use pip
RUN pip install -r requirements.txt

# copy in the frontend requirements next so that we benefit from caching
COPY *bower* /usr/src/app/
# install node/npm from conda, use them to bootstrap bower, then user bower
# to install our true frontend dependencies, and finally remove node/npm
# from the image since they're only required at build time
RUN conda install nodejs -y -c conda-forge && \
    npm install -g bower && \
    bower install --allow-root && \
    npm uninstall -g bower && \
    conda remove nodejs -y

# install yarnitor last to get the most caching
COPY . /usr/src/app
RUN pip install .

# never run as the default root user
RUN useradd -ms /bin/bash yarnitor
USER yarnitor
# configure gunicorn concurrency
ENV WEB_CONCURRENCY 5
CMD ["gunicorn", "-b", "0.0.0.0:8080", "--access-logfile", "-", "yarnitor:app"]
