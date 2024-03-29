"""
adapted from https://github.com/tensorflow/datasets/blob/master/tensorflow_datasets/core/file_adapters.py
"""

"""Adapters for file formats."""

import abc
import enum
import os

from typing import Any, ClassVar, Dict, Iterable, List, Optional, Type
from fileio.core.libs import TF_FUNC
from fileio.core import type_utils

ExamplePositions = List[Any]


class FileFormat(enum.Enum):
    """Format of the record files.
    The values of the enumeration are used as filename endings/suffix.
    """
    TFRECORD = 'tfrecord'


DEFAULT_FILE_FORMAT = FileFormat.TFRECORD


class FileAdapter(abc.ABC):
    """Interface for Adapter objects which read and write examples in a format."""

    FILE_SUFFIX: ClassVar[str]

    @classmethod
    @abc.abstractmethod
    def make_tf_data(
            cls,
            filename: type_utils.PathLike,
            buffer_size: Optional[int] = None,
    ) -> TF_FUNC.data.Dataset:
        """Returns TensorFlow Dataset comprising given record file."""
        raise NotImplementedError()

    @classmethod
    @abc.abstractmethod
    def write_examples(
            cls,
            path: type_utils.PathLike,
            iterator: Iterable[type_utils.KeySerializedExample],
    ) -> Optional[ExamplePositions]:
        """Write examples from given iterator in given path.
        Args:
            path: Path where to write the examples.
            iterator: Iterable of examples.
        Returns:
            List of record positions for each record in the given iterator. In case of
            TFRecords, does not return anything.
        """
        raise NotImplementedError()


class TfRecordFileAdapter(FileAdapter):
    """File adapter for TFRecord file format."""

    FILE_SUFFIX = 'tfrecord'

    @classmethod
    def make_tf_data(
            cls,
            filename: type_utils.PathLike,
            buffer_size: Optional[int] = None,
    ) -> TF_FUNC.data.Dataset:
        """Returns TensorFlow Dataset comprising given record file."""
        return TF_FUNC.data.TFRecordDataset(filename, buffer_size=buffer_size)

    @classmethod
    def write_examples(
            cls,
            path: type_utils.PathLike,
            iterator: Iterable[type_utils.KeySerializedExample],
    ) -> Optional[ExamplePositions]:
        """Write examples from given iterator in given path.
        Args:
            path: Path where to write the examples.
            iterator: Iterable of examples.
        Returns:
            None
        """
        with TF_FUNC.io.TFRecordWriter(os.fspath(path)) as writer:
            for _, serialized_example in iterator:
                writer.write(serialized_example)
            writer.flush()



def _to_bytes(key: type_utils.Key) -> bytes:
    """Convert the key to bytes."""
    if isinstance(key, int):
        return key.to_bytes(128, byteorder='big')    # Use 128 as this match md5
    elif isinstance(key, bytes):
        return key
    elif isinstance(key, str):
        return key.encode('utf-8')
    else:
        raise TypeError(f'Invalid key type: {type(key)}')


# Create a mapping from FileFormat -> FileAdapter.
ADAPTER_FOR_FORMAT: Dict[FileFormat, Type[FileAdapter]] = {
    FileFormat.TFRECORD: TfRecordFileAdapter,
}

_FILE_SUFFIX_TO_FORMAT = {
    adapter.FILE_SUFFIX: file_format
    for file_format, adapter in ADAPTER_FOR_FORMAT.items()
}


def file_format_from_suffix(file_suffix: str) -> FileFormat:
    """Returns the file format associated with the file extension (`tfrecord`)."""
    if file_suffix not in _FILE_SUFFIX_TO_FORMAT:
        raise ValueError('Unrecognized file extension: Should be one of '
                                         f'{list(_FILE_SUFFIX_TO_FORMAT.values())}')
    return _FILE_SUFFIX_TO_FORMAT[file_suffix]


def is_example_file(filename: str) -> bool:
    """Whether the given filename is a record file."""
    return any(f'.{adapter.FILE_SUFFIX}' in filename
                         for adapter in ADAPTER_FOR_FORMAT.values())