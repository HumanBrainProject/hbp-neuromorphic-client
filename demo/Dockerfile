FROM neuralensemble/simulation:pynn0110

USER root
RUN apt-get update --fix-missing; apt-get -y -q install python3-pip supervisor nginx-extras git unzip curl cron
USER docker

COPY requirements.txt /home/docker/packages/
COPY requirements-provider.txt /home/docker/packages/
# we access the NMPI client from the system Python, not the virtual env the simulator runs in
RUN /usr/bin/python3 -m pip install -r /home/docker/packages/requirements.txt
RUN /usr/bin/python3 -m pip install -r /home/docker/packages/requirements-provider.txt

RUN mkdir /home/docker/runner; mkdir /home/docker/bin; mkdir /home/docker/data
COPY nmpi /home/docker/packages/nmpi
COPY bin/nmpi_run.py /home/docker/bin/nmpi_run.py
COPY demo/nmpi.cfg /home/docker/runner/nmpi.cfg

ENV PYTHONPATH  /home/docker/packages
ENV NMPI_CONFIG /home/docker/runner/nmpi.cfg

# Check for jobs every minute
RUN crontab -l | { cat; echo "* * * * * PYTHONPATH=/home/docker/packages NMPI_CONFIG=/home/docker/runner/nmpi.cfg /usr/bin/python3 /home/docker/bin/nmpi_run.py >> /home/docker/cron.log 2>> /home/docker/cron.log"; } | crontab -

USER root
RUN echo "daemon off;" >> /etc/nginx/nginx.conf
RUN rm /etc/nginx/sites-enabled/default
COPY demo/nginx-app.conf /etc/nginx/sites-enabled/
COPY demo/supervisor-app.conf /etc/supervisor/conf.d/

EXPOSE 80 443

CMD ["supervisord", "-n", "-c", "/etc/supervisor/conf.d/supervisor-app.conf"]