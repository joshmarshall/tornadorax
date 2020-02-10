# TornadoRax

[![Build Status](https://travis-ci.org/joshmarshall/tornadorax.png?branch=master)](https://travis-ci.org/joshmarshall/tornadorax)

A collection of OpenStack wrappers for the Tornado web framework. While this
is intended to support all OpenStack providers and services over time,
honestly it is mostly used against Rackspace, hence the "rax". It may
transition to tornadostax over time.

Right now it just includes queues (Zaqar eventually) and basic cloud
files object operations.  Still way early. There are some example scripts
in the examples/ directory.

(Only supports 3.5+ as of Feb 2020.)
