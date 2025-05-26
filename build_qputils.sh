#!/bin/bash

TARGET_SERVER="user"

make_dir_copy_server_file(){
  echo ""; echo "====== Copy file structures ...";
  # Ensure the destination directory exists
  mkdir -p ./qp/server_$TARGET_SERVER

  cp -r ./server_$TARGET_SERVER/* ./qp/server_$TARGET_SERVER/
  cp ./qputils/* ./qp/server_$TARGET_SERVER/
  cp ./server_env_vars.sh ./qp/server_$TARGET_SERVER/
  echo ""; echo "====== Copy file structures done...";
}




