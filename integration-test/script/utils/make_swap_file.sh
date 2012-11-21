#!/bin/bash

echo *******Must be in sudo mode to execute********
mkdir ${1}/swap
dd if=/dev/zero of=${1}/swap/swap0 bs=1M count=102400
chmod 600 ${1}/swap/swap0
mkswap ${1}/swap/swap0
swapon ${1}/swap/swap0

