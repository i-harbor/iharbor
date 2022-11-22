

class MultipartPartsManager:

    def binary_search(self, arr, num, start, end):
        """
        二分法查询

        :return:(
            index: int,        # -1：未找到； >=0: part在列表中的索引
            part: dict,        # part信息
            start: int,         # 查询结束时 搜索的索引位置
            end: int            # 查询结束时 搜索的索引位置
        )
        """
        mid = (start + end) // 2

        if start > end:
            if end < 0:
                end = 0
            start, end = end, start
            return -1, None, start, end

        if start == end:
            if arr[mid]['PartNumber'] == num:
                return mid, arr[mid], start, end
            return -1, None, start, end

        if arr[mid]['PartNumber'] == num:
            return mid, arr[mid], start, end
        elif arr[mid]['PartNumber'] > num:
            return self.binary_search(arr, num, start, mid - 1)
        elif arr[mid]['PartNumber'] < num:
            return self.binary_search(arr, num, mid + 1, end)

    def query_part_info(self, num: int, parts: list):
        """
        查询指定编号的part

        :return:(
            part,       # part信息; None(不存在)
            int         # part在列表的索引; None(不存在)
        )
        """
        if not parts:
            return None, None

        if num < len(parts):
            if parts[num - 1]['PartNumber'] == num:
                return parts[num - 1], num - 1

        index, p, start, end = self.binary_search(arr=parts, num=num, start=0, end=len(parts) - 1)
        if index < 0:
            index = None

        return p, index

    def insert_part_into_list(self, part, parts_arr):
        """
        :return: (
            bool,       # True(插入)；False(替换)
            list
        )
        """
        if not parts_arr:
            parts_arr.append(part)
            return True, parts_arr

        num = part['PartNumber']
        index, p, start, end = self.binary_search(arr=parts_arr, num=num, start=0, end=len(parts_arr) - 1)

        if index > -1:
            parts_arr[index] = part
            return False, parts_arr

        # 在该位置的块 大于 插入的块
        elif parts_arr[start]['PartNumber'] > num:
            # 如果start=0
            if start - 1 < 0:
                parts_arr.insert(start, part)
            if num > parts_arr[start-1]['PartNumber']:
                parts_arr.insert(start, part)

        elif parts_arr[start]['PartNumber'] < num:
            # 在该位置的块 小于 插入的块
            if start + 1 <= len(parts_arr) - 1 and num < parts_arr[start+1]['PartNumber']:
                parts_arr.insert(start + 1, part)
            else:
                parts_arr.append(part)

        return True, parts_arr
