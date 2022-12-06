import asyncio
from fileio import File
from fileio.utils import logger, timed, timer


gcs_path = 'gs://path-to-gcs-bucket'
aws_path = 's3://path-to-aws-bucket'

file_data = "hello world"
file_name = "iotest.txt"

gcs = File(gcs_path)
aws = File(aws_path)

"""
Test Ops
"""
# @timed
def test_write():
    logger.info("[Sync] Running test_write")

    gcs_file = gcs.joinpath(file_name)
    t = timer()
    logger.info(f'[GCS] Writing to {gcs_file.as_posix()}')
    gcs_file.write_text(file_data)

    assert gcs_file.exists()
    assert gcs_file.is_file()
    assert gcs_file.is_dir() == False
    assert gcs_file.read_text() == file_data
    cksum = gcs_file.checksum
    logger.info(f'[GCS] File Checksum: {cksum}')
    gcs_file.rm()
    timer(t, f'[GCS] Write Time: {gcs_file.as_posix()}')

    
    aws_file = aws.joinpath(file_name)
    t = timer()
    logger.info(f'[AWS] Writing to {aws_file.as_posix()}')
    aws_file.write_text(file_data)
    assert aws_file.exists()
    assert aws_file.is_file()
    assert aws_file.is_dir() == False
    assert aws_file.read_text() == file_data
    cksum = aws_file.checksum
    logger.info(f'[AWS] File Checksum: {cksum}')
    aws_file.rm()

    timer(t, f'[AWS] Write Time: {aws_file.as_posix()}')


# @timed
def test_remove():
    logger.info("[Sync] Running test_remove")

    gcs_file = gcs.joinpath(file_name)
    t = timer()
    logger.info(f'[GCS] Removing {gcs_file.as_posix()}')
    gcs_file.write_text(file_data)
    gcs_file.rm()
    assert gcs_file.exists() == False

    gcs_file.write_text(file_data)
    gcs_file.unlink()
    assert gcs_file.exists() == False

    gcs_file.write_text(file_data)
    gcs_file.rm_file()
    assert gcs_file.exists() == False
    timer(t, '[GCS] test_remove')

    aws_file = aws.joinpath(file_name)
    t = timer()
    logger.info(f'[AWS] Removing {aws_file.as_posix()}')
    aws_file.write_text(file_data)
    aws_file.rm()
    assert aws_file.exists() == False

    aws_file.write_text(file_data)
    aws_file.unlink()
    assert aws_file.exists() == False

    aws_file.write_text(file_data)
    aws_file.rm_file()
    assert aws_file.exists() == False
    timer(t, '[AWS] test_remove')



"""
Async Methods
"""


@timed
async def async_test_remove():
    logger.info("[Async] Running test_remove")

    gcs_file = gcs.joinpath(file_name)
    t = timer()
    logger.info(f'[GCS] Removing {gcs_file.as_posix()}')
    await gcs_file.async_write_text(file_data)
    await gcs_file.async_rm()
    assert await gcs_file.async_exists() == False

    await gcs_file.async_write_text(file_data)
    await gcs_file.async_unlink()
    assert await gcs_file.async_exists() == False

    await gcs_file.async_write_text(file_data)
    await gcs_file.async_rm_file()
    assert await gcs_file.async_exists() == False
    timer(t, '[GCS] test_remove')

    aws_file = aws.joinpath(file_name)
    t = timer()
    logger.info(f'[AWS] Removing {aws_file.as_posix()}')
    
    await aws_file.async_write_text(file_data)
    await aws_file.async_rm()
    assert await aws_file.async_exists() == False

    await aws_file.async_write_text(file_data)
    await aws_file.async_unlink()
    assert await aws_file.async_exists() == False

    await aws_file.async_write_text(file_data)
    await aws_file.async_rm_file()
    assert await aws_file.async_exists() == False

    timer(t, '[AWS] test_remove')



@timed
async def async_test_write():
    logger.info("[Async] Running test_write")

    gcs_file = gcs.joinpath(file_name)
    t = timer()
    logger.info(f'[GCS] Writing to {gcs_file.as_posix()}')
    await gcs_file.async_write_text(file_data)

    assert await gcs_file.async_exists()
    assert await gcs_file.async_is_file()
    assert await gcs_file.async_is_dir() == False
    assert await gcs_file.async_read_text() == file_data
    cksum = await gcs_file.async_get_checksum()
    logger.info(f'[GCS] File Checksum: {cksum}')
    await gcs_file.async_rm()
    timer(t, f'[GCS] Write Time: {gcs_file.as_posix()}')
    
    aws_file = aws.joinpath(file_name)
    t = timer()
    logger.info(f'[AWS] Writing to {aws_file.as_posix()}')
    await aws_file.async_write_text(file_data)

    assert await aws_file.async_exists()
    assert await aws_file.async_is_file()
    assert await aws_file.async_is_dir() == False
    assert await aws_file.async_read_text() == file_data
    cksum = await aws_file.async_get_checksum()
    logger.info(f'[AWS] File Checksum: {cksum}')
    await aws_file.async_rm()


async def run_tests():
    test_write()
    test_remove()

    await async_test_write()
    await async_test_remove()

async def debug_test():
    gcs_file = gcs.joinpath(file_name)
    await gcs_file.async_write_text(file_data)

    print(await gcs_file.async_exists())
    print(await gcs_file.async_is_file())
    print(await gcs_file.async_is_dir())
    print(await gcs_file.async_cat())
    print(await gcs_file.async_cat_file(as_bytes=False))

    print(await gcs_file.async_info())
    
    print('----')

    print(gcs_file.exists())
    print(gcs_file.is_file())
    print(gcs_file.is_dir())
    print(gcs_file.cat_file())

    print('---')
    print(gcs_file.info())

    # print(gcs.glob('*.txt'))
    # print(gcs.glob('**/*'))

    print('---')
    # print(await gcs.async_glob('*.txt'))
    # print(await gcs.async_glob('**/*'))

    # aws_file = aws.joinpath(file_name)
    # await aws_file.async_write_text(file_data)

    # print(await aws_file.async_exists())
    # print(await aws_file.async_is_file())
    # print(await aws_file.async_is_dir())
    # print(await aws_file.async_cat())
    # print(await aws_file.async_cat_file(as_bytes=False))

    # print(await aws_file.async_info())
    
    # print('----')

    # print(aws_file.exists())
    # print(aws_file.is_file())
    # print(aws_file.is_dir())
    # print(aws_file.cat_file())
    # print(aws_file.info())

    # print('---')

    # print(aws.glob('*.txt'))
    # # print(gcs.glob('**/*'))

    # print('---')
    # print(await aws.async_glob('*.txt'))

# open()

if __name__ == '__main__':
    # anyio.run(run_tests)
    #asyncio.run(run_tests())
    asyncio.run(debug_test())