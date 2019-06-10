#!/bin/bash
ecloud/env/bin/python ecloud/worker.py $(cat worker_id) $(cat broker_url) > error_log

