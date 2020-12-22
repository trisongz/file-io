

def check_imports():
    avail = {'colab': False, 'libs': {}}
    try:
        import google.colab
        avail['colab'] = True
    except ImportError:
        avail['colab'] = False
    
    try:
        import tensorflow as tf
        avail['libs']['tensorflow'] = True
    except ImportError:
        avail['libs']['tensorflow'] = False

    try:
        from google.cloud import storage
        avail['libs']['gcs'] = True
    except ImportError:
        avail['libs']['gcs'] = False

    try:
        import smart_open
        avail['libs']['smart_open'] = True
    except ImportError:
        avail['libs']['smart_open'] = False

    try:
        import tqdm.auto
        avail['libs']['tqdm'] = True
    except ImportError:
        avail['libs']['tqdm'] = False
    
    try:
        import boto3
        avail['libs']['boto3'] = True
    except ImportError:
        avail['libs']['boto3'] = False

    return avail


