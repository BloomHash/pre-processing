#!/bin/bash

${SPARK_HOME}/bin/spark-submit --class $1 --deploy-mode cluster --supervise $2 $3 $4
