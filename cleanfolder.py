import os
import json
import pandas as pd
import shutil
import dejavuV2
import hashlib

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

working_directory = '/run/media/megavolts/CHUGINADAK/musics/'


def list_relpath(working_directory):
    file_set = set()
    for root, dirs, files in os.walk(working_directory, topdown=False):
        for f in files:
            rel_path = os.path.relpath(root, working_directory)
            file_set.add(os.path.join(rel_path, f))
    return sorted(file_set)


def list_abspath(working_directory):
    file_set = set()
    for root, _, files in os.walk(working_directory, topdown=False):
        for f in files:
            file_set.add(os.path.join(root, f))
    return sorted(file_set)


def list_subdir(working_directory, lowest=False):
    flist = set()
    for root, dirs, files in os.walk(working_directory, topdown=False):
        if lowest:
            if files and not dirs:
                flist.add(root)
        else:
            for dir in dirs:
                flist.add(os.path.join(root, dir))
    return sorted(flist)


def clean_dir(file_set, working_directory):
    for file in file_set:
        if file.split('/')[-1] in ['desktop.ini', '.working_directory']:
            os.remove(os.path.abspath(os.path.join(working_directory, file)))
            logger.info('Removing %s' % file)
            file_set.remove(file)
        elif file.split('/')[-1][0] == '.':
            os.remove(os.path.abspath(os.path.join(working_directory, file)))
            logger.info('Removing %s' % file)
            file_set.remove(file)
        elif '.trash-' in file or '.Trash' in file:
            os.remove(os.path.abspath(os.path.join(working_directory, file)))
            logger.info('Removing %s' % file)
            file_set.remove(file)
    return file_set


def detox(string_value):
    """
    Normalizes string, converts to lowercase, removes non-alpha characters, but keep period (. and slahs)
    and converts spaces to hyphens.
    """

    string_value = unicodedata.normalize('NFKD', string_value).encode('ascii', 'ignore').decode().lower().replace(" ", "_")
    string_value = re.sub('[^\w\s(?./)-]', '_', string_value).strip().lower().replace("_-_", "-")
    return string_value


def is_music(file):
    if file.lower().endswith(('jpg', 'jpeg', 'png', 'm3u', 'wpl', 'zpl', 'db', 'ncd', 'txt', 'm4p', 'cue', 'zip',
                              'md5', 'bmp', 'sfv', 'pamp', 'pdf', 'jpe')):
        return False
    else:
        return True


def delete_files(rel_filepath, working_directory, dirname='00-to_delete'):
    del_filepath = os.path.join(working_directory, '..', dirname,
                                os.path.join(os.path.relpath(rel_filepath, working_directory)))
    del_dir = os.path.dirname(del_filepath)
    if not os.path.isdir(del_dir):
        os.makedirs(del_dir)
    shutil.move(rel_filepath, del_filepath)
    logger.info(file.split('/')[-1] + ' deleted to ' + del_filepath)


def hash_file(filepath, hashdb=None):
    hashes = hashlib.sha1()
    with open(filepath, 'rb') as fpair:
        hashes.update(fpair.read())

    if hashdb is None:
        hashdb = pd.DataFrame(columns=['hash', 'sid'])

    if hashes.hexdigest() in hashdb['hash'].tolist():
        duplicate = filepath
    else:
        hashdb = hashdb.append({'hash': hashes.hexdigest(), 'sid': filepath}, ignore_index=True)
        duplicate = None
    logger.debug("%d in %s" % (hashdb.__len__(), filepath))
    return hashdb, duplicate


def hash_match(source, target):
    hashlist = []
    for file in [source, target]:
        temp_hash = hashlib.sha1()
        with open(file, 'rb') as fpair:
            temp_hash.update(fpair.read())
            hashlist.append(temp_hash)
    if hashlist[0].hexdigest() == hashlist[1].hexdigest():
        return True
    else:
        return False


logging.info('detoxing file name')
file_set = list_relpath(working_directory)
file_set = clean_dir(file_set, working_directory)
for file in file_set:
    source = os.path.join(working_directory, file)
    target = os.path.join(working_directory, detox(file))
    target_dir = os.path.dirname(target)
    if not os.path.isdir(target_dir):  # create a working_directory if there is none
        os.makedirs(target_dir)
        shutil.move(source, target)
        logging.info(file + ' moved to ' + target)
    elif not os.path.exists(target):
        shutil.move(source, target)
        logging.info(file + ' moved to ' + target)
    elif source == target:
        logging.info(file + ' is identical to target')
        pass
    elif not is_music(source):
        if hash_match(source, target):
            delete_files(source, working_directory, dirname='00-to_delete')
        else:
            shutil.move(source, target)
            logging.info(file, ' moved to ', target)
    elif dejavuV2.file_matches(source, target, limit=1):
        logging.info(source.split('/')[-1] + ' is similar to ' + target.split('/')[-1])
        delete_files(source, working_directory, dirname='00-to_delete')
    else:
        delete_files(source, working_directory, dirname='00-to_check')

logging.info("CLEANING TREE")
for directory, dirs, _ in os.walk(working_directory, topdown=False):
    logger.info(directory)
    if not dirs:
        dirname = '00-to_delete'
    else:
        dirname = '00-to_check'
    flist = list_abspath(directory)
    fgdb = None
    hashdb = None
    for file in flist:
        if not(os.path.exists(file)):
            logging.info(file + ' does not exist')
        elif is_music(file):
            fgdb, duplicate = dejavuV2.fingerprint_db(file, fgdb=fgdb, limit=2)
            if duplicate is None:
                logger.info(file.split('/')[-1] + ' preserved')
            else:
                delete_files(file, working_directory, dirname=dirname)
        else:
            hashdb, duplicate = hash_file(file, hashdb)
            if duplicate is None:
                logger.info(file.split('/')[-1] + ' preserved')
            else:
                delete_files(file, working_directory, dirname=dirname)