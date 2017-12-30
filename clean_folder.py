import os
import shutil
import hashlib
#work_dir = '/home/megavolts/Desktop/test'
#work_dir = '/home/megavolts/media/Music/0-to_sort'
work_dir = '/run/media/megavolts/UNIMAK-1to/musics'
flist = []

for root, dirs, files in os.walk(work_dir, topdown=False):
    for f in files:
        rel_path = os.path.relpath(root, work_dir)
        flist.append(os.path.join(rel_path, f))

#for f in flist:
for f in flist:
    source_filepath = os.path.join(work_dir, f)
    if not f.islower():
        target_filepath = os.path.join(work_dir, f.lower())
        target_dirpath = os.path.dirname(target_filepath)

        if not os.path.isdir(target_dirpath):  # create a directory if there is none
            os.makedirs(target_dirpath)
            shutil.move(source_filepath, target_filepath)
            print(source_filepath, ' moved to ', target_filepath)
        elif os.path.exists(target_filepath):
            # compare file
            source_hash = hashlib.sha1()
            with open(source_filepath, 'rb') as sfile:
                buf = sfile.read()
                source_hash.update(buf)
            target_hash = hashlib.sha1()
            with open(target_filepath, 'rb') as sfile:
                buf = sfile.read()
                target_hash.update(buf)

            if source_hash.hexdigest() == target_hash.hexdigest():
                os.remove(source_filepath)
                print(source_filepath, ' deleted')
        else:
            shutil.move(source_filepath, target_filepath)
            print(source_filepath, ' moved to ', target_filepath)


