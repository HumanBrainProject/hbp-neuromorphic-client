stages:
  - build
  - test

test_main:
  stage: test
  only:
    variables:
      - $CI_COMMIT_BRANCH == "main"
  script:
    - python3 -m pip install -r requirements.txt
    - python3 -m pip install -r requirements-provider.txt
    - python3 -m pip install -r requirements-testing.txt
    - python3 -m pytest -v --cov=nmpi --cov-report=term
  tags:
    - docker-runner
  image: docker-registry.ebrains.eu/neuromorphic/python:3.9-slim-git


build_demo_server:
  stage: build
  only:
    variables:
      - $CI_COMMIT_BRANCH == "main"
  script:
    - docker build -f demo/Dockerfile -t docker-registry.ebrains.eu/neuromorphic/demo .
    - echo $DOCKER_REGISTRY_USER
    - docker login -u $DOCKER_REGISTRY_USER -p $DOCKER_REGISTRY_SECRET docker-registry.ebrains.eu
    - docker push docker-registry.ebrains.eu/neuromorphic/demo
  tags:
    - shell-runner
