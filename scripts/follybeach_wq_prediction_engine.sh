#!/bin/bash

source /usr/local/virtualenv/pyenv-3.8.5/bin/activate

cd /home/xeniaprod/scripts/FollyBeach-WaterQuality/scripts;

python follybeach_wq_prediction_engine.py --ConfigFile=/home/xeniaprod/scripts/FollyBeach-WaterQuality/config/folly_prediction_engine.ini >> /home/xeniaprod/tmp/log/follybeach_wq_prediction_engine_sh.log 2>&1
