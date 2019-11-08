
import os
import shutil
count=0
src_dir = "C:/users/ianpe/desktop/csv_files"
dst_dir = "C:/users/ianpe/desktop/all_files"
for root, dirs, files in os.walk(src_dir):
    for f in files:
        if f.endswith('.csv'):
            dst_file = os.path.join(root, f)
            new_file_name = str(count)+f
            new_dst_file_name = os.path.join(dst_dir, new_file_name)
            print(dst_file)
            print(new_dst_file_name)
            os.rename(dst_file, new_dst_file_name)
            count += 1
