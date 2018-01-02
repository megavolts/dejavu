import warnings
import os
import pandas as pd

warnings.filterwarnings("ignore")

from dejavuV2 import decoder
from dejavuV2 import fingerprint

import logging


def _fingerprint_worker(filename, limit=None, song_name=None):
    # Pool.imap sends arguments as tuples so we have to unpack
    # them ourself.
    try:
        filename, limit = filename
    except ValueError:
        pass

    logger = logging.getLogger(__name__)

    songname, extension = os.path.splitext(os.path.basename(filename))
    song_name = song_name or songname
    channels, Fs, file_hash = decoder.read(filename, limit=limit)
    result = set()
    channel_amount = len(channels)

    for channeln, channel in enumerate(channels):
        # TODO: Remove prints or change them into optional logging.
        logger.debug("Fingerprinting channel %d/%d for %s" % (channeln + 1, channel_amount, filename))
        hashes = fingerprint.fingerprint(channel, Fs=Fs)
        logger.debug("Finished channel %d/%d for %s" % (channeln + 1, channel_amount, filename))
        result |= set(hashes)

    return song_name, result, file_hash


def fingerprint_file(filepath, limit=None, song_name=None):

    logger = logging.getLogger(__name__)

    songname = decoder.path_to_songname(filepath)
    song_hash = decoder.unique_hash(filepath)
    song_name = song_name or songname
    # don't refingerprint already fingerprinted files
    song_name, hashes, file_hash = _fingerprint_worker(
        filepath,
        limit=limit,
        song_name=song_name
    )
    return song_name, hashes, file_hash


def fingerprint_directory(fgdb, songdb, path, extensions, limit, nprocesses=None):
    logger = logging.getLogger(__name__)


    filenames_to_fingerprint = []
    for filename, _ in decoder.find_files(path, extensions):
        # # don't refingerprint already fingerprinted files
        # if decoder.unique_hash(filename) in self.songhashes_set:
        #     print("%s already fingerprinted, continuing..." % filename)
        #     continue

        filenames_to_fingerprint.append(filename)

    for filename in filenames_to_fingerprint:
        song_name, hashes, file_hash = fingerprint_file(filename, limit=limit, song_name=None)

        songdb = songdb.append({'song_name': song_name, 'hash': file_hash, 'filepath': filename}, ignore_index=True)
        sid = songdb.last_valid_index()

        for hash, offset in hashes:
            fgdb = fgdb.append({'hash': hash, 'sid': sid, 'offset': offset}, ignore_index=True)

    return fgdb, songdb


def return_matches(fgdb, hashes):
    """
    Return the (song_id, offset_diff) tuples associated with
    a list of (sha1, sample_offset) values.
    """
    # Create a dictionary of hash => offset pairs for later lookups
    mapper = {}
    for hash, offset in hashes:
        mapper[hash] = offset

    for values in mapper:
        for row in fgdb[fgdb.hash==values].iterrows():
            yield(row[1]['sid'], row[1]['offset']-mapper[hash])


def file_matches(source_filepath, target_filepath, limit=None):
    _, hashes_source, _ = fingerprint_file(source_filepath, limit=limit)
    _, hashes_target, _ = fingerprint_file(target_filepath, limit=limit)
    logger = logging.getLogger(__name__)

    fgdb = pd.DataFrame(columns=['hash', 'sid', 'offset'])
    for hash, offset in hashes_target:
        fgdb = fgdb.append({'hash': hash, 'sid': 0, 'offset': offset}, ignore_index=True)
    logger.debug("%d in %s" % (fgdb.__len__(), source_filepath))

    matches = return_matches(fgdb, hashes_source)

    # align by diffs
    diff_counter = {}
    for tup in matches:
        sid, diff= tup
        if diff not in diff_counter:
            diff_counter[diff] = 0
        diff_counter[diff] +=1

    count = 0
    for diff in diff_counter:
        if -25 < diff < 25:
            count += diff_counter[diff]

    if sum(diff_counter.values()) == 0:
        return 0
    elif sum(diff_counter.values()) <= 50*limit and count/sum(diff_counter.values()) > 0.95:
        return 1
    elif sum(diff_counter.values()) <= 100*limit and count/sum(diff_counter.values()) > 0.9:
        return 1
    if sum(diff_counter.values()) > 100*limit:
        return 1
    else:
        return 0


def fingerprint_db(filepath, fgdb=None, limit=None):
    logger = logging.getLogger(__name__)
    # if limit < 3:
    #     limit = 3
    #     logger.warning('limit set to the minimum of 3 seconds')
    _, hashes, _ = fingerprint_file(filepath, limit=limit)

    if fgdb is None:
        fgdb = pd.DataFrame(columns=['hash', 'sid', 'offset'])

    res = match_fingerprint_db(fgdb, hashes, limit=limit)
    if res.__len__() > 0:
        duplicate = filepath
        logger.info(filepath.split('/')[-1] +' matched fingerprint with ' + '., '.join(res))
    else:
        for hash, offset in hashes:
            fgdb = fgdb.append({'hash': hash, 'sid': filepath, 'offset': offset}, ignore_index=True)
        duplicate = None
    logger.debug("%d in %s" % (fgdb.__len__(), filepath))
    return fgdb, duplicate


def match_fingerprint_db(fgdb, hashes, limit):
    matches = return_matches(fgdb, hashes)

    # align by diffs
    diff_counter = {}
    for tup in matches:
        sid, diff = tup
        if diff not in diff_counter:
            diff_counter[diff] = {}
        if sid not in diff_counter[diff]:
            diff_counter[diff][sid] = 0
        diff_counter[diff][sid] += 1

    count = {}
    for diff in diff_counter:
        for sid in diff_counter[diff]:
            #if -25 < diff_counter[diff][sid] < 25:
            if sid not in count:
                count[sid] = 0
            count[sid] += diff_counter[diff][sid]

    res = []
    for sid in count:
        if count[sid] > max(10, 5*limit):
            res.append(sid)
    return res

# source = '/home/megavolts/git/dejavu/mp3/sample/air/The Virgin Suicides/01 Air feat. Gordon Tracks - Playground Love.mp3'
# target = '/home/megavolts/git/dejavu/mp3/sample/air/The Virgin Suicides/05 Air - Dark Messages.mp3'
#
# file_matches(source, target)
#
# folder = '/home/megavolts/git/dejavu/mp3/sample/air/'
# filepath = '/home/megavolts/git/dejavu/mp3/sample/air/The Virgin Suicides/01 Air feat. Gordon Tracks - Playground Love.mp3'
#
#
# limit = 5
# song_name = None
#
#
# path = folder
# extensions = ['.mp3']
#
# fgdb = pd.DataFrame(columns=['hash', 'sid','offset'])
# songdb = pd.DataFrame(columns=['song_name', 'hash', 'filepath'])
#
# fgdb, songdb = fingerprint_directory(fgdb, songdb, path, extensions, limit=10)
#
# # find matches
# _, hashes, _ = fingerprint_file(filepath, limit=5)
# matches = return_matches(fgdb, hashes)
# align_matches(matches)
# songdb[songdb.index==1]
# # align matches
# filenames = []
# for filename, _ in decoder.find_files(path, extensions):
#     filenames.append(filename)
#
# for songfn in filename:
#     _, hashes, _ = fingerprint_file(filepath, limit=5)
#     matches = return_matches(fgdb, hashes)
#     align_matches(matches)
