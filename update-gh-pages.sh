#!/bin/bash
cd docs
make clean
make html
cp -r * ../../mrQA-gh-pages/
cd ../../mrQA-gh-pages/
git add .
git commit -m "Update docs"
git push

