import gzip
import bz2
import lzma
import brotli


class CompressHandler:
    """压缩处理"""

    def gzipcompress(self, data, compresslevel=9):
        """gzip 压缩"""
        if not compresslevel:
            compresslevel = 9

        try:
            bytes_obj = gzip.compress(data=data, compresslevel=compresslevel)
        except Exception as e:
            raise e
        return bytes_obj

    def gzipdempresshandler(self, data):
        """解压缩处理"""
        with gzip.GzipFile(fileobj=data) as f:
            return f.read()

    def gzipdecompress(self, data):
        """gzip 解压"""
        try:
            # bytes_obj = gzip.decompress(data=data)
            bytes_obj = self.gzipdempresshandler(data=data)
        except Exception as e:
            raise e
        return bytes_obj

    def bz2compress(self, data, compresslevel=9):
        """bz2 压缩"""
        if not compresslevel:
            compresslevel = 9

        try:
            bytes_obj = bz2.compress(data=data, compresslevel=compresslevel)
        except Exception as e:
            raise e
        return bytes_obj

    def bz2decompresshandler(self, file):
        """bz2解压缩处理"""
        with bz2.open(file, 'rb') as f:
            return f.read()

    def bz2decompress(self, data):
        """bz2 解压"""
        try:
            # bytes_obj = bz2.decompress(data=data)
            bytes_obj = self.bz2decompresshandler(file=data)
        except Exception as e:
            raise e
        return bytes_obj

    def lzmacompress(self, data, compresslevel=6):
        """lzma 压缩"""
        # preset 默认压缩等级 6
        if not compresslevel:
            compresslevel = 6

        try:
            bytes_obj = lzma.compress(data=data, preset=compresslevel)
        except lzma.LZMAError as e:
            raise e
        return bytes_obj

    def lzmadecompresshandler(self, file):
        """lzma解压缩处理"""
        with lzma.open(file) as f:
            return f.read()

    def lzmadecompress(self, data):
        """
        lzma 解压
        CHECK_CRC64: 64 位循环冗余检查 默认
        """
        try:
            # bytes_obj = lzma.decompress(data=data)
            bytes_obj = self.lzmadecompresshandler(file=data)
        except lzma.LZMAError as e:
            raise
        return bytes_obj

    def brotlicompress(self, data, compresslevel=11):
        """brotli 压缩"""
        # quality 默认压缩等级 11
        if not compresslevel:
            compresslevel = 11

        try:
            bytes_obj = brotli.compress(string=data, quality=compresslevel)
        except brotli.error as e:
            raise e
        return bytes_obj

    def brotlidecompress(self, data):
        """brotli 解压"""
        try:
            bytes_obj = brotli.decompress(string=data.read())
        except brotli.error as e:
            data.close()
            raise e
        data.close()
        return bytes_obj

    def compress(self, data, compresstype, compresslevel=None):
        """压缩数据"""

        if compresstype == 'gzip':
            compress_data = self.gzipcompress(data=data, compresslevel=compresslevel)
        elif compresstype == 'bz2': # bzip2
            compress_data = self.bz2compress(data=data, compresslevel=compresslevel)
        elif compresstype == 'lzma':
            compress_data = self.lzmacompress(data=data, compresslevel=compresslevel)
        elif compresstype == 'br':
            compress_data = self.brotlicompress(data=data, compresslevel=compresslevel)
        else:
            raise ValueError(f"不支持 {compresstype} 类型压缩")

        return compress_data

    def decompress(self, data, decompresstype):
        """解压数据"""
        if decompresstype == 'gzip':
            decompress_data = self.gzipdecompress(data=data)
        elif decompresstype == 'bz2':  # bzip2
            decompress_data = self.bz2decompress(data=data)
        elif decompresstype == 'lzma':
            decompress_data = self.lzmadecompress(data=data)
        elif decompresstype == 'br':
            decompress_data = self.brotlidecompress(data=data)
        else:
            raise ValueError(f"不支持 {decompresstype} 类型解压缩")

        return decompress_data

    def compresstypelist(self):
        """压缩类型列表"""
        return ['gzip', 'bz2', 'lzma', 'br']

    def checkcompresstype(self, contentencoding):
        compress_type_list = self.compresstypelist()

        if not contentencoding:
            return None
        elif contentencoding in compress_type_list:
            return True
        else:
            raise ValueError(f"不支持 {contentencoding} 类型解压缩")


if __name__ == '__main__':
    s_in = b"mountUnmountedrclonemount"
    c = CompressHandler()
    try:
        f = c.compress(data=s_in, compresstype='gzip')
        d = c.decompress(data=f, decompresstype='gzip')
    except Exception as e:
        print(f"e = {str(e)}")

