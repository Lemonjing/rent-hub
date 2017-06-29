# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
import os
import sys


this_file_dir = os.path.split(os.path.realpath(__file__))[0]
config_file_path = os.path.join(this_file_dir, 'config.ini')

print this_file_dir
print config_file_path

print sys.getdefaultencoding()
