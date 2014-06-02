#!/bin/bash
git clone https://github.com/torvalds/linux
cd linux
make defconfig
make -j4
cd ..
rm -rf linux
