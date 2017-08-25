FROM continuumio/miniconda3:latest

# make shell directories so that other commands work
RUN mkdir -p /usr/src/app/yarnitor/static
WORKDIR /usr/src/app

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

# never run as the default root user
RUN useradd -ms /bin/bash yarnitor

# install yarnitor last to get the most caching
COPY . /usr/src/app
RUN pip install .

USER yarnitor
