import multiprocessing
import warnings
import os

warnings.filterwarnings("ignore")

folder = '/home/megavolts/git/dejavu/mp3/sample/air/'
filepath = '/home/megavolts/git/dejavu/mp3/sample/air/The Virgin Suicides/01 Air feat. Gordon Tracks - Playground Love.mp3'

from dejavuV2 import decoder
from dejavuV2 import fingerprint

limit = 5

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
