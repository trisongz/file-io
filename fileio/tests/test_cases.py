from fileio import File


# Manipulating Filename Test Cases
gsfname = 'gs://random_bucket/dir/filepath.txt'
res = File.mod_fname(gsfname, ext='json', directory='/content', create_dirs=False, prefix='test_', suffix='_001')
print('Modified Filename: ', res)

res = File.mod_fname(gsfname, ext='json', create_dirs=False, prefix='test_', suffix='_001')
print('Modified Filename: ', res)
