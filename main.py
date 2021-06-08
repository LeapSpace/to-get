import requests
import math
# noinspection PyCompatibility
import urllib.parse
# noinspection PyCompatibility
import concurrent.futures
import sys
import os
from typing import List


class Download(object):
    BLOCK_SIZE = 1024 * 1024
    BUFFERED_SIZE = 8 * 1024
    TMP_DIR = "tmp"

    def __init__(self, target: str, block_size: int = None, pool_size: int = 10, dst_dir: str = ".",
                 dst_name: str = None):
        self._target = target
        if not dst_name:
            self._filename = self._get_filename()
        else:
            self._filename = dst_name
        self._pool_size = pool_size
        if block_size:
            self._block_size = block_size
        else:
            self._block_size = Download.BLOCK_SIZE
        self._segments = []
        self._size = 0
        self._dir = os.path.abspath(dst_dir)
        self._tmp = self._dir + os.path.sep + Download.TMP_DIR
        if not os.path.exists(self._tmp):
            os.mkdir(self._tmp)
        self._pool = concurrent.futures.ThreadPoolExecutor(max_workers=self._pool_size)
        self._task = []
        self._part_num = 0

    def _get_filename(self):
        _res = urllib.parse.urlparse(self._target)
        return _res.path.split("/")[-1]

    def _test_filesize(self):
        if not self._test_support_range():
            raise RuntimeError("target server does not support range parameter!")
        _res = requests.head(self._target, allow_redirects=False)
        # print(_res.status_code)
        if _res.is_redirect or _res.is_permanent_redirect:
            _res = requests.get(_res.next.url)
        if _res.status_code == 200:
            content_length = _res.headers.get("Content-Length")
            if content_length is not None:
                self._size = int(content_length)
            _res.close()

    def _test_support_range(self):
        _headers = {
            "Range": "bytes=0-0"
        }
        _res = requests.get(self._target, headers=_headers)
        if 206 != _res.status_code or int(_res.headers.get("Content-Length").strip()) != 1:
            return False
        return True

    def _get_part(self, part: int):
        print("part: {0}\n".format(part))
        _offset = part * self._block_size
        _end = _offset + self._block_size - 1
        _last_part_size = self._size % self._block_size
        _tmp_filename = self._tmp + os.path.sep + self._filename + ".part." + str(part)
        if os.path.exists(_tmp_filename) and os.stat(_tmp_filename).st_size in (self._block_size, _last_part_size):
            print("part {0} is already ok.".format(part))
            return
        _headers = {
            "Range": "bytes=" + str(_offset) + "-" + str(_end)
        }
        try:
            print("start downloading file part: %d" % part)
            r = requests.get(self._target, headers=_headers)
            if 206 == r.status_code:
                with open(_tmp_filename, "wb") as f:
                    f.write(r.content)
                f.close()
                print("part success: " + str(part))
        except Exception:
            self._task.append(self._pool.submit(self._get_part, part))
            print("_part failed: " + str(part))

    def _merge(self):
        _src_file = self._tmp + os.path.sep + self._filename
        _dst_file = self._dir + os.path.sep + self._filename
        cur = 0
        cur_file = _src_file + ".part." + str(cur)
        _tmp_files = []
        _success = False
        with open(_dst_file, 'wb') as f:
            while True:
                if cur >= self._part_num:
                    _success = True
                    break
                print("current :{0}".format(cur_file))
                if os.path.exists(cur_file):
                    _tmp_files.append(cur_file)
                    with open(cur_file, 'rb') as cf:
                        while True:
                            bs = cf.read(Download.BUFFERED_SIZE)
                            if bs:
                                f.write(bs)
                            else:
                                break
                        cf.close()
                    cur += 1
                    cur_file = _src_file + ".part." + str(cur)
                else:
                    print("{0} not exists!".format(cur_file))
                    break
            f.close()
        if _success:
            Download._clear_tmp_files(_tmp_files)

    @staticmethod
    def _clear_tmp_files(file_lst: List):
        for f in file_lst:
            os.remove(f)

    def run(self):
        print("test file size start")
        try:
            self._test_filesize()
        except Exception as e:
            print(e)
            import sys
            sys.exit(-1)
        print("test file size end")
        if self._size > 0:
            nums = math.ceil(self._size * 1.0 / self._block_size)
            self._part_num = nums
            for i in range(nums):
                self._task.append(self._pool.submit(self._get_part, i))
            concurrent.futures.wait(self._task, return_when=concurrent.futures.ALL_COMPLETED)
            self._pool.shutdown()
            self._merge()
        print("file download completed")


if __name__ == '__main__':
    down = Download(sys.argv[1])
    down.run()
