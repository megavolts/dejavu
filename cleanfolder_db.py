import multiprocessing
import warnings
import os
import traceback
import sys
import pandas as pd


warnings.filterwarnings("ignore")

folder = '/home/megavolts/git/dejavu/mp3/sample/air/'
filepath = '/home/megavolts/git/dejavu/mp3/sample/air/The Virgin Suicides/01 Air feat. Gordon Tracks - Playground Love.mp3'

from dejavuV2 import decoder
from dejavuV2 import fingerprint
from itertools import zip_longest


limit = 5
song_name = None


def _fingerprint_worker(filename, limit=None, song_name=None):
    # Pool.imap sends arguments as tuples so we have to unpack
    # them ourself.
    try:
        filename, limit = filename
    except ValueError:
        pass

    songname, extension = os.path.splitext(os.path.basename(filename))
    song_name = song_name or songname
    channels, Fs, file_hash = decoder.read(filename, limit)
    result = set()
    channel_amount = len(channels)

    for channeln, channel in enumerate(channels):
        # TODO: Remove prints or change them into optional logging.
        print("Fingerprinting channel %d/%d for %s" % (channeln + 1,
                                                       channel_amount,
                                                       filename))
        hashes = fingerprint.fingerprint(channel, Fs=Fs)
        print("Finished channel %d/%d for %s" % (channeln + 1, channel_amount,
                                                 filename))
        result |= set(hashes)

    return song_name, result, file_hash


def fingerprint_file(filepath, song_name=None):
    songname = decoder.path_to_songname(filepath)
    song_hash = decoder.unique_hash(filepath)
    song_name = song_name or songname
    # don't refingerprint already fingerprinted files
    song_name, hashes, file_hash = _fingerprint_worker(
        filepath,
        limit,
        song_name=song_name
    )

nprocesses=None
path = folder
extensions = '.mp3'

fgdb = pd.DataFrame(columns=[])
songdb = pd.DataFrame(columns=['sid', 'hash'])

def fingerprint_directory(db, path, extensions, limit, nprocesses=None):
    # Try to use the maximum amount of processes if not given.
    try:
        nprocesses = nprocesses or multiprocessing.cpu_count()
    except NotImplementedError:
        nprocesses = 1
    else:
        nprocesses = 1 if nprocesses <= 0 else nprocesses

    pool = multiprocessing.Pool(nprocesses)

    filenames_to_fingerprint = []
    for filename, _ in decoder.find_files(path, extensions):

        # # don't refingerprint already fingerprinted files
        # if decoder.unique_hash(filename) in self.songhashes_set:
        #     print("%s already fingerprinted, continuing..." % filename)
        #     continue

        filenames_to_fingerprint.append(filename)

    # Prepare _fingerprint_worker input
    worker_input = zip(filenames_to_fingerprint,
                       [limit] * len(filenames_to_fingerprint))

    # Send off our tasks
    iterator = pool.imap_unordered(_fingerprint_worker,
                                   worker_input)

    # Loop till we have all of them
    while True:
        try:
            song_name, hashes, file_hash = iterator.next()
        except multiprocessing.TimeoutError:
            continue
        except StopIteration:
            break
        except:
            print("Failed fingerprinting")
            # Print traceback because we can't reraise it here
            traceback.print_exc(file=sys.stdout)
        else:
            songdb.append({'a':song_name, 'b':file_hash})

            sid = self.db.insert_song(song_name, file_hash)

            self.db.insert_hashes(sid, hashes)
            self.db.set_song_fingerprinted(sid)
            self.get_fingerprinted_songs()

    pool.close()
    pool.join()



hashes0 = hashes

for hash, offset in hashes:
    values.append((hash, sid, offset))

def grouper(iterable, n, fillvalue=None):
    args = [iter(iterable)] * n
    return ([_f for _f in values if _f] for values
            in zip_longest(fillvalue=fillvalue, *args))



def return_matches(hashes0, hashes):
    """
    Return the (song_id, offset_diff) tuples associated with
    a list of (sha1, sample_offset) values.
    """
    # Create a dictionary of hash => offset pairs for later lookups
    mapper = {}
    for hash, offset in hashes:
        mapper[hash.upper()] = offset

    # Get an iteratable of all the hashes we need
    values = mapper.keys()

    with self.cursor() as cur:
        for split_values in grouper(values, 1000):
            # Create our IN part of the query
            query = self.SELECT_MULTIPLE
            query = query % ', '.join(['UNHEX(%s)'] * len(split_values))

            cur.execute(query, split_values)

            for hash, sid, offset in cur:
                # (sid, db_offset - song_sampled_offset)
                yield (sid, offset - mapper[hash])

filename=filepath
songname = decoder.path_to_songname(filepath)
song_hash = decoder.unique_hash(filepath)
song_name = song_name or songname
# don't refingerprint already fingerprinted files
song_name, hashes, file_hash = _fingerprint_worker(
    filepath,
    limit,
    song_name=song_name
)