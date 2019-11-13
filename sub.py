# This block will unzip any .gz files

# import zipfile,fnmatch,os
# import gzip,shutil
# rootPath = r"C:\users\ianpe\desktop\reviews"
# pattern = 'reviews.csv.gz'
# for root, dirs, files in os.walk(rootPath):
#     for filename in fnmatch.filter(files, pattern):
#         print(os.path.join(root, filename))
#         os.chdir(root)
#         with gzip.open(filename,'rb') as f_in:
#             with open(filename[:-3], 'wb') as f_out:
#                 shutil.copyfileobj(f_in,f_out)
        # zipfile.ZipFile(os.path.join(root, filename)).extractall(os.path.join(root, os.path.splitext(filename)[0]))

# This block will move any of the target csvs to another directory (since all are at first stored in sub sub sub sub sub directories)
import os
import shutil
count=0
src_dir = "C:/users/ianpe/desktop/reviews"
dst_dir = "C:/users/ianpe/desktop/reviews_csvs"
for root, dirs, files in os.walk(src_dir):
    for f in files:
        if f.endswith('reviews.csv'):
            dst_file = os.path.join(root, f)
            new_file_name = str(count)+f
            new_dst_file_name = os.path.join(dst_dir, new_file_name)
            print(dst_file)
            print(new_dst_file_name)
            os.rename(dst_file, new_dst_file_name)
            count += 1
            # shutil.copy(os.path.join(root,new_dst_file_name), dst_dir)