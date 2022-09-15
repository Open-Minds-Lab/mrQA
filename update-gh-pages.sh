#!/bin/bash
# git clone -b gh-pages --single-branch git@github.com:Open-Minds-Lab/mrQA.git .
cd docs
make clean
make html
cp -r * ../../mrQA-gh-pages/
cd ../../mrQA-gh-pages/
git add .
git commit -m "Update docs"
git push

