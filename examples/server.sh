#! /bin/sh

cd pingpong_app
mkdir -p pingpong_sdk
thrift -out pingpong_sdk --gen py:new_style  pingpong.thrift
cd ..

gunicorn_thrift pingpong_app.app:app -c gunicorn_config.py --bind 0.0.0.0:8880
