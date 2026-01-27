import zipfile
import os
import shutil
import argparse
from datetime import datetime

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", help="Path to the zip file")
    args = parser.parse_args()

    zip_path = args.file
    if not zip_path or not os.path.exists(zip_path):
        print(f"File {zip_path} not found. Skipping.")
        return

    extract_dir = zip_path.rsplit('.', 1)[0]
    os.makedirs(extract_dir, exist_ok=True)

    # Thực hiện giải nén
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)
    print(f"Successfully unzipped to {extract_dir}")

    # Xóa zip an toàn
    if os.path.exists(zip_path):
        os.remove(zip_path)

    # Dọn dẹp folder cũ (Keep current one)
    root = os.path.dirname(zip_path)
    current_date_str = os.path.basename(extract_dir).split("_")[2]
    for name in os.listdir(root):
        folder_path = os.path.join(root, name)
        if os.path.isdir(folder_path) and "_bk_" in name:
            try:
                folder_date = name.split("_")[2]
                if folder_date < current_date_str:
                    shutil.rmtree(folder_path)
                    print(f"Cleaned up old folder: {name}")
            except: continue

if __name__ == "__main__":
    main()