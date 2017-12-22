import os
import shutil
import dejavuV2
import json
import logging

import unicodedata
import re


import logging.config
# Enable logging
LOG_CFG = 'logging.json'
if os.path.exists(LOG_CFG):
    with open(LOG_CFG, 'rt') as f:
        config = json.load(f)
    logging.config.dictConfig(config)
else:
    logging.basicConfig(level=logging.DEBUG)

logger = logging.getLogger(__name__)

work_dir = '/home/megavolts/media/Music/0-to_sort/'
#work_dir = '/run/media/megavolts/UNIMAK-1to/musics/0-sorted'
flist = []

for root, dirs, files in os.walk(work_dir, topdown=False):
    for f in files:
        rel_path = os.path.relpath(root, work_dir)
        if f in ['desktop.ini', '.directory']:
            abs_filepath = os.path.join(root, f)
            os.remove(abs_filepath)
            logger.info('Removing %s' % abs_filepath)
        if f[0] == '.':
            abs_filepath = os.path.join(root, f)
            os.remove(abs_filepath)
            logger.info('Removing %s' % abs_filepath)
        else:
            flist.append(os.path.join(rel_path, f))


def detox(val):
    """
    Normalizes string, converts to lowercase, removes non-alpha characters, but keep period (. and slahs)
    and converts spaces to hyphens.
    """

    val = unicodedata.normalize('NFKD', val).encode('ascii', 'ignore').decode().lower().replace(" ", "_")
    val = re.sub('[^\w\s(?./)-]', '_', val).strip().lower().replace("_-_", "-")
    return val


for f in flist:
    source_filepath = os.path.join(work_dir, f)
    target_filepath = os.path.join(work_dir, detox(f))
    target_dirpath = os.path.dirname(target_filepath)

    if not os.path.isdir(target_dirpath):  # create a directory if there is none
        os.makedirs(target_dirpath)
        shutil.move(source_filepath, target_filepath)
        print(source_filepath, ' moved to ', target_filepath)
    elif os.path.exists(target_filepath):
        # compare file
        if source_filepath.lower().endswith('jpg') or source_filepath.lower().endswith('png'):
            del_filepath = os.path.join(work_dir, '..', '00-to_delete', f)
            del_dir = os.path.dirname(del_filepath)
            if not os.path.isdir(del_dir):
                os.makedirs(del_dir)
            shutil.move(source_filepath, del_filepath)
            print(source_filepath + ' moved to ' + del_filepath)

        elif dejavuV2.file_matches(source_filepath, target_filepath, limit=5):
            #val = input('Delete source [Y/n]')
            #if val.lower() == 'y':
            del_filepath = os.path.join(work_dir, '..', '00-to_delete', f)
            del_dir = os.path.dirname(del_filepath)
            if not os.path.isdir(del_dir):
                os.makedirs(del_dir)
            shutil.move(source_filepath, del_filepath)
            print(source_filepath + ' moved to ' + del_filepath)
        else:
            del_filepath = os.path.join(work_dir, '..', '00-to_check', f)
            del_dir = os.path.dirname(del_filepath)
            if not os.path.isdir(del_dir):
                os.makedirs(del_dir)
            shutil.move(source_filepath, del_filepath)
            print(source_filepath + ' moved to ' + del_filepath)
    else:
        shutil.move(source_filepath, target_filepath)
        print(source_filepath, ' moved to ', target_filepath)
