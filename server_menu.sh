#!/bin/bash

# Source the build script
source ./build_qputils.sh

echo "=== Server Menu ==="
echo "1. Build and Copy Files"
echo "2. Exit"
read -p "Choose an option: " choice

case $choice in
  1)
    make_dir_copy_server_file

    source ./qp/server_$TARGET_SERVER/server_env_vars.sh

    python3 ./qp/server_$TARGET_SERVER/db_generate.py
    ;;
  2)
    echo "Exiting."
    exit 0
    ;;
  *)
    echo "Invalid option."
    ;;
esac
