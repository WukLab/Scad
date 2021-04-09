## Disagg-Serverless tests

This directory contains a Makefile containing all of the commands
necessary to deploy and stand up the openwhisk cluster locally from scratch,
along with all of the pre-requisites.

After building and standing up the cluster, you can run tests using the
faas-profiler (not included with this project). The basic instructions are
provided below.

### Useful targets

- `make dist_docker`: compiles everything and builds the necessary docker containers for testing
- `make test_ow*`: This should consist of 2 targets `test_ow` and
  `test_ow_base`. They build, run, and profile everything, and then present the
  results.
- 