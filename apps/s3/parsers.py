from rest_framework_xml.parsers import XMLParser


class S3XMLParser(XMLParser):
    """
    S3XML解析器，xml string to dict

    字段的键值key不含xml命名空间
    """
    def _xml_convert(self, element):
        data = self._xml_convert_no_root(element)
        parent_tag = element.tag.rsplit('}', 1)[-1]
        return {parent_tag: data}

    def _xml_convert_no_root(self, element):
        """
        convert the xml `element` into the corresponding python object
        """
        children = list(element)

        if len(children) == 0:
            return self._type_convert(element.text)
        else:
            data = {}
            for child in children:
                val = self._xml_convert_no_root(child)
                tag: str = child.tag
                tag = tag.rsplit('}', 1)[-1]
                if tag not in data:
                    data[tag] = val
                else:
                    item = data[tag]
                    if not isinstance(item, list):
                        data[tag] = [item, val]
                    else:
                        data[tag].append(val)

            return data


