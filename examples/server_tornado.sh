#! /bin/sh

cd pingpong_app
mkdir -p pingpong_sdk
thrift -out pingpong_sdk --gen py:new_style  pingpong.thrift
mkdir -p pingpong_sdk_tornado
thrift -out pingpong_sdk_tornado --gen py:tornado pingpong.thrift
cd ..

gunicorn_thrift pingpong_app.app:app -c gunicorn_config_framed.py --bind 0.0.0.0:8880
