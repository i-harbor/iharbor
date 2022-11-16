

class MultipartPartsManager:
    # 二分查询
    def retrieve_part_info(self, arr, num, start, end):
        result = -1
        mid = (start + end) // 2

        if start > end:
            if end < 0:
                end = 0
            start, end = end, start
            return result, None, start, end

        if start == end:
            if arr[mid]['PartNumber'] == num:
                return mid, arr[mid], start, end
            return result, None, start, end

        if arr[mid]['PartNumber'] == num:
            return mid, arr[mid], start, end
        elif arr[mid]['PartNumber'] > num:
            return self.retrieve_part_info(arr, num, start, mid - 1)
        elif arr[mid]['PartNumber'] < num:
            return self.retrieve_part_info(arr, num, mid + 1, end)

    # 获取某一块
    def query_part_info(self, num, parts):
        if not parts:
            return None, None

        if num < len(parts):
            if parts[num - 1]['PartNumber'] == num:
                return parts[num - 1], num - 1

        index, p, start, end = self.retrieve_part_info(arr=parts, num=num, start=0, end=len(parts) - 1)
        return p, index

    def list_insert_part(self, part, parts_arr):
        if not parts_arr:
            parts_arr.append(part)
            return True, parts_arr

        num = part['PartNumber']
        index, p, start, end = self.retrieve_part_info(arr=parts_arr, num=num, start=0, end=len(parts_arr) - 1)

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





