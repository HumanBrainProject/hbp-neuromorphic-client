This folder contains files for a demonstration "neuromorphic computing service provider",
which accepts PyNN jobs, but just runs them with NEST.

The queue name is "Demo".


Running this on a VM:
    sudo docker run -d --name nmpi_demo -p 80:80 -p 443:443 -v /etc/letsencrypt/:/etc/letsencrypt/ -v /mnt/data/:/home/docker/data docker-registry.ebrains.eu/neuromorphic/demo