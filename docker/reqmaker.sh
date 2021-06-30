virtualenv .venv/v-env
source .venv/v-env/bin/activate
pip3 install --upgrade pip
pip3 install wheel
pip3 install Cython
pip3 install beautifulsoup4
pip3 install regex
pip3 install sudachipy
pip3 install sudachidict_core

pip3 freeze > requirements.txt