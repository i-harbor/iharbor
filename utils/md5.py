import hashlib
import base64

EMPTY_HEX_MD5 = 'd41d8cd98f00b204e9800998ecf8427e'
EMPTY_BYTES_MD5 = hashlib.md5().digest()


class FileHashHandlerBase:
    """
    hash计算
    """
    def __init__(self):
        self.hash = None
        self.start_offset = 0       # 下次输入数据开始偏移量
        self.is_valid = True

    def __getattr__(self, item):
        return getattr(self.hash, item)

    def update(self, offset: int, data: bytes):
        """
        md5计算需要顺序输入文件的数据，否则MD5计算无效
        """
        if not self.is_valid:
            return

        data_len = len(data)
        if offset < 0:
            if data_len > 0:
                self.set_invalid()
            return

        if data_len == 0:
            return

        start_offset = self.start_offset
        if start_offset == offset:
            self.hash.update(data)
            self.start_offset = start_offset + data_len
            return
        elif start_offset < offset:    # 计算无效
            self.set_invalid()
            return

        will_offset = offset + data_len
        if will_offset <= start_offset:   # 数据已输入过了
            return

        cut_len = will_offset - start_offset
        self.hash.update(data[-cut_len:])   # 输入start_offset开始的部分数据
        self.start_offset = will_offset

    def set_invalid(self):
        self.is_valid = True


class FileMD5Handler(FileHashHandlerBase):
    """
    MD5计算
    """

    def __init__(self):
        super().__init__()
        self.hash = hashlib.md5()

    @property
    def hex_md5(self):
        if self.is_valid:
            return self.hash.hexdigest()

        return ''

    def set_invalid(self):
        self.is_valid = True


class Sha256Handler(FileMD5Handler):
    def __init__(self):
        super().__init__()
        self.hash = hashlib.sha256()


class S3ObjectMultipartETagHandler:
    """
    S3对象多部分上传ETag计算
    """

    def __init__(self):
        self.md5_hash = hashlib.md5()

    def update(self, md5_hex: str):
        self.md5_hash.update(md5_hex_to_bytes(md5_hex))

    def __getattr__(self, item):
        return getattr(self.md5_hash, item)

    @property
    def hex_md5(self):
        return self.md5_hash.hexdigest()


def md5_hex_to_bytes(s: str):
    return bytes.fromhex(s)


def chunks(fd, chunk_size=10 * 1024 ** 2):
    """
    Read the file and yield chunks of ``chunk_size`` bytes
    """
    try:
        fd.seek(0)
    except AttributeError:
        pass

    while True:
        d = fd.read(chunk_size)
        if not d:
            break
        yield d


def offset_chunks(fd, chunk_size=10 * 1024 ** 2):
    """
    Read the file and yield offset and chunk of ``chunk_size`` bytes
    :return:
        (offset: int, chunk: bytes) or None
    """
    try:
        fd.seek(0)
    except AttributeError:
        pass

    offset = 0
    while True:
        d = fd.read(chunk_size)
        if not d:
            break

        yield offset, d
        offset += len(d)


def calculate_md5(filename: str):
    with open(filename, 'rb') as fd:
        md5obj = hashlib.md5()
        for d in chunks(fd):
            md5obj.update(d)

        _hash = md5obj.hexdigest()

    return _hash


def to_b64(s: str):
    """
    base64编码字符串
    """
    bs = base64.b64encode(s.encode("utf-8"))
    return bs.decode("utf-8")


def from_b64(s: str):
    bs = base64.b64decode(s.encode("utf-8"))
    return bs.decode("utf-8")
