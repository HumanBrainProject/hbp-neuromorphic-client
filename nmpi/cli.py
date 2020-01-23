"""
Command-line tool for interacting with the Neuromorphic Computing Platform of the Human Brain Project.

Authors: Andrew P. Davison and Domenico Guarino, NeuroPSI, CNRS


Copyright 2016-2019 Andrew P. Davison and Domenico Guarino, Centre National de la Recherche Scientifique

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""

import os
import logging
import yaml
import click
import nmpi


INCOMPLETE_JOBS_FILE = ".incomplete_jobs.yml"

logging.basicConfig(filename='nmpi.log', level=logging.WARNING,
                    format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger("NMPI")
logger.setLevel(logging.INFO)


def load_config():
    if not os.path.exists("nmpi_config.yml"):
        raise click.ClickException("There is no config file (nmpi_config.yml) in this directory")
    with open("nmpi_config.yml") as fp:
        config = yaml.safe_load(fp)
    for required_field in ("username", "collab_id", "default_platform"):
        if required_field not in config:
            raise click.ClickException("{} must be defined in nmpi_config.yml".format(required_field))
    return config


def read_incomplete_jobs():
    if os.path.exists(INCOMPLETE_JOBS_FILE):
        with open(INCOMPLETE_JOBS_FILE) as fp:
            job_configs = yaml.safe_load(fp) or []
        return job_configs
    else:
        return []


def _url_from_env(server_env):
    if server_env == "production":
        job_service = "https://nmpi.hbpneuromorphic.eu/api/v2/"
    elif server_env == "staging":
        job_service = "https://nmpi-staging.hbpneuromorphic.eu/api/v2/"
    elif server_env.startswith("https"):
        job_service = server_env
    else:
        raise ValueError("--server-env option should be 'production', 'staging', or a valid server URL")
    return job_service


def write_incomplete_jobs(job_configs):
    with open(INCOMPLETE_JOBS_FILE, 'w') as fp:
        yaml.dump(job_configs, fp)


@click.option("--debug", is_flag=True)
@click.group()
def cli(debug):
    """Top-level command"""
    if debug:
        logger.setLevel(logging.DEBUG)


@cli.command()
@click.argument("script")
@click.option("-p", "--platform",
              help="SpiNNaker, BrainScaleS or Spikey")
@click.option("-t", "--tag", multiple=True,
              help="Add a tag to the job")
@click.option("-b", "--batch", is_flag=True,
              help="Submit job then return immediately")
@click.option("-o", "--output-dir",
              help="Output directory")
@click.option("-e", "--server-env", default="production")
def run(script, platform, batch, output_dir, tag, server_env):
    """
    Run a simulation/emulation
    """
    config = load_config()
    client = nmpi.Client(username=config["username"], job_service=_url_from_env(server_env))

    if os.path.exists(os.path.expanduser(script)):
        if os.path.isdir(script):
            source = script
            command = "run.py {system}"
        else:
            source = os.path.dirname(script)
            if len(source) == 0:
                source = "."
            command = "{} {{system}}".format(os.path.basename(script))
    else:
        raise click.ClickException("Script '{}' does not exist".format(script))

    job = client.submit_job(source,
                            platform=platform or config["default_platform"],
                            collab_id=config["collab_id"],
                            config=config.get("hardware_config", None),
                            inputs=None,
                            command=command,
                            tags=tag,
                            wait=not batch)

    output_dir = output_dir or config.get("default_output_dir", ".")
    if batch:
        # save job_id for later checking
        write_incomplete_jobs(read_incomplete_jobs() + [{"job_id": job, "output_dir": output_dir}])
    else:
        if job["status"] == 'finished':
            client.download_data(job, local_dir=output_dir)
        else:
            assert job["status"] == 'error'
        click.echo(job["log"])
        # todo: also write logs to file in output_dir


@click.option("-e", "--server-env", default="production")
@cli.command()
def check(server_env=None):
    """
    Check for completed jobs
    """
    # todo: add a "continuous" mode so that this can run as a background job
    #       this will require locking the incomplete jobs file - see https://filelock.readthedocs.io/
    config = load_config()
    client = nmpi.Client(username=config["username"], job_service=_url_from_env(server_env))

    incomplete_jobs = read_incomplete_jobs()
    completed_jobs = []
    for job_config in incomplete_jobs:
        job = client.get_job(job_config["job_id"])
        if job["status"] == "finished":
            client.download_data(job, local_dir=job_config["output_dir"])
            completed_jobs.append(job_config)
            click.echo("Job #{} complete".format(job["id"]))
        elif job["status"] == "error":
            completed_jobs.append(job_config)
            click.echo("Job #{} errored".format(job["id"]))
        with open(os.path.join(job_config["output_dir"], "job_{}.log".format(job["id"])), 'w') as fp:
            fp.write(job["log"])

    for job_config in completed_jobs:
        incomplete_jobs.remove(job_config)
    write_incomplete_jobs(incomplete_jobs)


if __name__ == "__main__":
    cli()
