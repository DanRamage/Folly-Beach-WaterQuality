#!/bin/bash

source /usr/local/virtualenv/pyenv-2.7.11/bin/activate

cd /home/xeniaprod/scripts/FollyBeach-WaterQuality/scripts;

python FollyRipCurrentProcess.py --ConfigFile=/home/xeniaprod/scripts/FollyBeach-WaterQuality/config/FollyRipCurrent.ini > /home/xeniaprod/tmp/log/follybeach_ripcurrent_scraper_sh.log 2>&1
