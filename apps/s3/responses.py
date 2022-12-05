from django.http import HttpResponse, StreamingHttpResponse
from rest_framework_xml.renderers import XMLRenderer


class XMLResponse(HttpResponse):
    """
    An HTTP response class that consumes data to be serialized to XML.

    :param data: Data to be dumped into json. By default only ``dict`` objects
      are allowed to be passed due to a security flaw before EcmaScript 5. See
      the ``safe`` parameter for more information.
    :param encoder: Should be a xml encoder class. Defaults to
      ``rest_framework_xml.renderers.XMLRenderer``.
    :param safe: Controls if only ``dict`` objects may be serialized. Defaults
      to ``True``.
    """

    def __init__(self, data, encoder=XMLRenderer, safe=True, **kwargs):
        if safe and not isinstance(data, dict):
            raise TypeError(
                'In order to allow non-dict objects to be serialized set the '
                'safe parameter to False.'
            )
        kwargs.setdefault('content_type', 'application/xml')
        data = encoder().render(data=data)
        super().__init__(content=data, **kwargs)


class IterResponse(StreamingHttpResponse):
    """
    响应返回数据是迭代器
    """
    def __init__(self, iter_content, **kwargs):
        super().__init__(**kwargs)
        self.iter_content = iter_content

    def __iter__(self):
        return self.iter_content

