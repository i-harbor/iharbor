from io import StringIO

from django.utils.encoding import force_str
from django.utils.xmlutils import SimplerXMLGenerator
from rest_framework.renderers import BaseRenderer
from rest_framework_xml.renderers import XMLRenderer


class CusXMLRenderer(XMLRenderer):
    def __init__(self, root_tag_name: str = 'root', item_tag_name: str = "list-item"):
        self.root_tag_name = root_tag_name
        self.item_tag_name = item_tag_name


class ListObjectsV2XMLRenderer(XMLRenderer):
    def __init__(self, root_tag_name: str = 'ListBucketResult'):
        self.root_tag_name = root_tag_name
        self.item_tag_name = "list-item"
        self.cur_item_tag_name = self.item_tag_name

    def _to_xml(self, xml, data):
        if isinstance(data, (list, tuple)):
            for item in data:
                xml.startElement(self.cur_item_tag_name, {})
                self._to_xml(xml, item)
                xml.endElement(self.cur_item_tag_name)

        elif isinstance(data, dict):
            for key, value in data.items():
                if key in ['Contents', 'CommonPrefixes']:
                    self.cur_item_tag_name = key
                    self._to_xml(xml, value)
                    self.cur_item_tag_name = self.item_tag_name
                else:
                    xml.startElement(key, {})
                    self._to_xml(xml, value)
                    xml.endElement(key)

        elif data is None:
            # Don't output any value
            pass

        else:
            xml.characters(force_str(data))


class NoRootListXMLRenderer(BaseRenderer):
    """
    没有根节点的列表xml渲染器.
    """
    def __init__(self, item_tag_name: str = "list-item"):
        self.item_tag_name = item_tag_name

    def render(self, data, accepted_media_type=None, renderer_context=None):
        """
        Renders `data` into serialized XML.
        """
        if data is None:
            return ""

        stream = StringIO()

        xml = SimplerXMLGenerator(stream, self.charset)
        xml.startDocument()

        self._to_xml(xml, data)

        xml.endDocument()
        return stream.getvalue()

    def _to_xml(self, xml, data):
        if isinstance(data, (list, tuple)):
            for item in data:
                xml.startElement(self.item_tag_name, {})
                self._to_xml(xml, item)
                xml.endElement(self.item_tag_name)

        elif isinstance(data, dict):
            for key, value in data.items():
                xml.startElement(key, {})
                self._to_xml(xml, value)
                xml.endElement(key)

        elif data is None:
            # Don't output any value
            pass

        else:
            xml.characters(force_str(data))


class CommonXMLRenderer(BaseRenderer):
    """
    列表项的渲染方式: key as xml item_tag_name,

    {a: [1, 2]}:
        <a>1</a>
        <a>2</a>
    """
    media_type = "application/xml"

    def __init__(self, root_tag_name: str = 'root', with_xml_declaration=True):
        self.root_tag_name = root_tag_name
        self.item_tag_name = 'item_tag_name'
        self.with_xml_declaration = with_xml_declaration

    def render(self, data, accepted_media_type=None, renderer_context=None):
        """
        Renders `data` into serialized XML.
        """
        if data is None:
            return ""

        stream = StringIO()

        xml = SimplerXMLGenerator(stream, self.charset)
        if self.with_xml_declaration:
            xml.startDocument()

        xml.startElement(self.root_tag_name, {})
        self._to_xml(xml, data)
        xml.endElement(self.root_tag_name)
        xml.endDocument()
        return stream.getvalue()

    def _to_xml(self, xml, data):
        if isinstance(data, (list, tuple)):
            for item in data:
                xml.startElement(self.item_tag_name, {})
                self._to_xml(xml, item)
                xml.endElement(self.item_tag_name)

        elif isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, (list, tuple)):
                    self.item_tag_name = key
                    self._to_xml(xml, value)
                else:
                    xml.startElement(key, {})
                    self._to_xml(xml, value)
                    xml.endElement(key)

        elif data is None:
            # Don't output any value
            pass

        else:
            xml.characters(force_str(data))


class ListObjectsV1XMLRenderer(CommonXMLRenderer):
    def __init__(self):
        super().__init__(root_tag_name='ListBucketResult')
