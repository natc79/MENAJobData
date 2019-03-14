import os
from config import FileConfig


def check_data_directory():
    """Check to make sure correct file structure exists for data processing."""
    
    print("Check directories")
    dirtype = {}
    dirtype['data/external'] = ['olx','tanqeeb','wuzzuf']
    dirtype['data/interim'] = ['olx','tanqeeb','wuzzuf']
    dirtype['data/processed'] = ['finaldb']
    dirtype['figures'] = ['olx','tanqeeb','wuzzuf']
    
    for dir, dirlist in dirtype.items():
        dirpath = FileConfig.datapath
        if not os.path.exists(os.path.join(dirpath, dir)):
            print("Making directory %s" % (os.path.join(dirpath, dir)))
            os.makedirs(os.path.join(dirpath, dir))
        else:
            print("Directory %s already exists" % (os.path.join(dirpath, dir)))
        for d in dirlist:
            if not os.path.exists(os.path.join(dirpath, dir, d)):
                os.makedirs(os.path.join(dirpath, dir, d))
                
if __name__ == "__main__":
    check_data_directory()