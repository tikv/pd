#!/usr/bin/env bash
../bin/pd-ctl -u 127.0.0.1:2379 member list
../bin/pd-ctl -u 127.0.0.1:2379 health